// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

#[allow(unused_import)] // TODO: remove it.

use std::cmp::Reverse;
use std::fmt::{self, Display, Formatter};
use std::fs::{self, Metadata};
use std::io::{self, ErrorKind, Read, Write};
use std::fs::{File, OpenOptions};
use std::path::PathBuf;
use std::time::Instant;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::{error, result, str, thread, time, u64};

use kvproto::raft_serverpb::{SnapshotCFFile, SnapshotMeta};
use protobuf::RepeatedField;
use rocksdb::{DBCompressionType, EnvOptions, IngestExternalFileOptions, SstFileWriter};
use kvproto::metapb::Region;
use kvproto::raft_serverpb::RaftSnapshotData;
use protobuf::Message;
use raft::eraftpb::Snapshot as RaftSnapshot;
use rocksdb::{CFHandle, Writable, WriteBatch, DB};

use raftstore::store::engine::{Iterable, Snapshot as DbSnapshot};
use raftstore::store::keys::{self, enc_end_key, enc_start_key};

use raftstore::store::metrics::{SNAPSHOT_BUILD_TIME_HISTOGRAM, SNAPSHOT_CF_KV_COUNT,
                                SNAPSHOT_CF_SIZE};
use raftstore::store::peer_storage::JOB_STATUS_CANCELLING;
use raftstore::Result as RaftStoreResult;
use raftstore::errors::Error as RaftStoreError;
use raftstore::store::Msg;
use raftstore::store::util::check_key_in_region;
use storage::{CfName, CF_DEFAULT, CF_LOCK, CF_WRITE};
use util::HandyRwLock;
use util::codec::bytes::{BytesEncoder, CompactBytesDecoder};
use util::collections::{HashMap, HashMapEntry as Entry};
use util::io_limiter::{IOLimiter, LimitWriter};
use util::rocksdb::{prepare_sst_for_ingestion, validate_sst_for_ingestion};
use util::transport::SendCh;
use util::file::{delete_file_if_exist, file_exists, get_file_size, calc_crc32};
use util::rocksdb;
use util::rocksdb::get_fastest_supported_compression_type;
use util::time::duration_to_sec;


use crc::crc32::{self, Digest, Hasher32};


// Data in CF_RAFT should be excluded for a snapshot.
pub const SNAPSHOT_CFS: &[CfName] = &[CF_DEFAULT, CF_LOCK, CF_WRITE];

/// Name prefix for the self-generated snapshot file.
const SNAP_GEN_PREFIX: &str = "gen";
/// Name prefix for the received snapshot file.
const SNAP_REV_PREFIX: &str = "rev";

const TMP_FILE_SUFFIX: &str = ".tmp";
const SST_FILE_SUFFIX: &str = ".sst";
const CLONE_FILE_SUFFIX: &str = ".clone";

const DELETE_RETRY_MAX_TIMES: u32 = 6;
const DELETE_RETRY_TIME_MILLIS: u64 = 500;

pub const SNAPSHOT_VERSION: u64 = 2;
const META_FILE_SUFFIX: &str = ".meta";

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Abort {
            description("abort")
            display("abort")
        }
        TooManySnapshots {
            description("too many snapshots")
        }
        Other(err: Box<error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("snap failed {:?}", err)
        }
    }
}

pub type Result<T> = result::Result<T, Error>;

// CF_LOCK is relatively small, so we use plain file for performance issue.
#[inline]
fn plain_file_used(cf: &str) -> bool {
    cf == CF_LOCK
}

#[inline]
pub fn check_abort(status: &AtomicUsize) -> Result<()> {
    if status.load(Ordering::Relaxed) == JOB_STATUS_CANCELLING {
        return Err(Error::Abort);
    }
    Ok(())
}

#[derive(Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct SnapKey {
    pub region_id: u64,
    pub term: u64,
    pub idx: u64,
}

impl SnapKey {
    #[inline]
    pub fn new(region_id: u64, term: u64, idx: u64) -> SnapKey {
        SnapKey {
            region_id,
            term,
            idx,
        }
    }

    pub fn from_region_snap(region_id: u64, snap: &RaftSnapshot) -> SnapKey {
        let index = snap.get_metadata().get_index();
        let term = snap.get_metadata().get_term();
        SnapKey::new(region_id, term, index)
    }

    pub fn from_snap(snap: &RaftSnapshot) -> io::Result<SnapKey> {
        let mut snap_data = RaftSnapshotData::new();
        if let Err(e) = snap_data.merge_from_bytes(snap.get_data()) {
            return Err(io::Error::new(ErrorKind::Other, e));
        }

        Ok(SnapKey::from_region_snap(
            snap_data.get_region().get_id(),
            snap,
        ))
    }
}

impl Display for SnapKey {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}_{}_{}", self.region_id, self.term, self.idx)
    }
}

pub struct ApplyOptions {
    pub db: Arc<DB>,
    pub region: Region,
    pub abort: Arc<AtomicUsize>,
    pub write_batch_size: usize,
}

pub trait Snapshot: Read + Write + Send {
    fn path(&self) -> &str;
    fn total_size(&self) -> io::Result<u64>;
    fn apply(&self, options: ApplyOptions) -> Result<()>;
}

fn gen_snapshot_meta(cf_files: &[CfFile]) -> RaftStoreResult<SnapshotMeta> {
    let mut meta = Vec::with_capacity(cf_files.len());
    for cf_file in cf_files {
        if SNAPSHOT_CFS.iter().find(|&cf| cf_file.cf == *cf).is_none() {
            return Err(box_err!(
                "failed to encode invalid snapshot cf {}",
                cf_file.cf
            ));
        }

        let mut cf_file_meta = SnapshotCFFile::new();
        cf_file_meta.set_cf(cf_file.cf.to_owned());
        cf_file_meta.set_size(cf_file.size);
        cf_file_meta.set_checksum(cf_file.checksum);
        meta.push(cf_file_meta);
    }
    let mut snapshot_meta = SnapshotMeta::new();
    snapshot_meta.set_cf_files(RepeatedField::from_vec(meta));
    Ok(snapshot_meta)
}

fn check_file_size(path: &PathBuf, expected_size: u64) -> RaftStoreResult<()> {
    let size = get_file_size(path)?;
    if size != expected_size {
        return Err(box_err!(
            "invalid size {} for snapshot cf file {}, expected {}",
            size,
            path.display(),
            expected_size
        ));
    }
    Ok(())
}

fn check_file_checksum(path: &PathBuf, expected_checksum: u32) -> RaftStoreResult<()> {
    let checksum = calc_crc32(path)?;
    if checksum != expected_checksum {
        return Err(box_err!(
            "invalid checksum {} for snapshot cf file {}, expected {}",
            checksum,
            path.display(),
            expected_checksum
        ));
    }
    Ok(())
}

fn check_file_size_and_checksum<P: Into<Path>>(
    path: P,
    expected_size: u64,
    expected_checksum: u32,
) -> RaftStoreResult<()> {
    check_file_size(path, expected_size).and_then(|_| check_file_checksum(path, expected_checksum))
}

#[derive(Default)]
struct CfFile {
    pub cf: CfName,
    pub path: PathBuf,
    pub sst_writer: Option<SstFileWriter>,
    pub file: Option<File>,
    pub size: u64,
    pub written_size: u64,
    pub checksum: u32,
    pub write_digest: Option<Digest>,
}

#[derive(Default)]
struct MetaFile {
    pub meta: SnapshotMeta,
    pub path: PathBuf,
    pub file: Option<File>,
}

pub struct Snap {
    key: SnapKey,
    for_sending: bool,
    cf_files: Vec<CfFile>,
    cf_index: usize,
    meta_file: MetaFile,
    snap_mgr_core: Arc<SnapManagerCore>,
}

impl Snap {
    fn new(key: &SnapKey, core: Arc<SnapManagerCore>, for_sending: bool) -> Snap {
        let mut cf_files = Vec::with_capacity(SNAPSHOT_CFS.len());
        for cf in SNAPSHOT_CFS {
            let cf_file = CfFile {
                cf: cf,
                path: core.cf_file_path(key, cf, for_sending),
                ..Default::default()
            };
            cf_files.push(cf_file);
        }

        let meta_file = MetaFile {
            path: core.meta_file_path(key, for_sending),
            ..Default::default(),
        };

        Snap {
            key: key.clone(),
            for_sending: for_sending,
            cf_files,
            cf_index: 0,
            meta_file,
            snap_mgr_core: core,
        }
    }

    fn load_disk_files(&mut self) -> Result<SnapshotMeta> {
        let meta = self.read_snapshot_meta()?;
        for cf in meta.get_cf_files() {
            let cf_name = cf.get_cf();
            let cf_size = cf.get_size();
            let cf_checksum = cf.get_checksum();
            let cf_path = self.snap_mgr_core.cf_file_path(&self.key, cf_name, self.for_sending);
            check_file_size_and_checksum(cf_path, cf_size, cf_checksum)?;
            self.switch_to_cf_file(cf_name)?;
            self.cf_files[self.cf_index].size = cf_size;
            self.cf_files[self.cf_index].checksum = cf_checksum;
        }
        Ok(meta)
    }

    fn register_for_build(&self) -> Result<()> {
        static err_msg: &'static str = "{} try to register Generating, but {} exists";
        let registry = self.snap_mgr_core.registry.wl();
        match registry.entry(self.key.clone()) {
            Entry::Occupied(e) => return box_err!(err_msg, self.key, entry),
            Entry::Vacant(e) => e.insert(SnapEntry::Generating),
        }
        Ok(())
    }

    fn register_for_sending(&self) -> Result<()> {
        static err_msg: &'static str = "{} try to register Sending, but {} exists";
        let registry = self.snap_mgr_core.registry.wl();
        if let Some(entry) = registry.get_mut(&self.key) {
            match entry {
                SnapEntry::Generating => {
                    let sender_count = AtomicUsize::new(1);
                    *entry = SnapEntry::Sending(sender_count);
                },
                SnapEntry::Sending(count) => count.fetch_add(1, Ordering::SeqCst),
                _ => return box_err!(err_msg, self.key, entry),
            }
            return Ok(());
        }
        return box_err!(err_msg, self.key, "None");
    }


    fn build_snapshot_data(mut self, region: &Region, db_snap: &DbSnapshot) -> Result<RaftSnapshotData>> {
        self.register_for_build()?;

        if !file_exists(self.meta_file.path) || !self.load_disk_files().is_ok() {
            self.init_for_building();
            let t = Instant::now();
            let snap_key_count = self.do_build(region, db_snap);
            SNAPSHOT_KV_COUNT_HISTOGRAM.observe(snap_key_count as f64);
            SNAPSHOT_SIZE_HISTOGRAM.observe(snap_size as f64);
            SNAPSHOT_BUILD_TIME_HISTOGRAM.observe(duration_to_sec(t.elapsed()) as f64);
        }

        let total_size = self.total_size()?;
        let snap_data = RaftSnapshotData::new();
        snap_data.set_file_size(total_size);
        snap_data.set_version(SNAPSHOT_VERSION);
        snap_data.set_meta(self.meta_file.meta.clone());

        info!(
            "[region {}] scan snapshot {}, size {}, key count {}, takes {:?}",
            region.get_id(),
            self.snap_mgr_core.display_path(&self.key, &self.for_sending),
            total_size,
            snap_key_count,
            t.elapsed()
        );
        Ok(snap_data)
    }

    fn init_for_building(&mut self, snap: &DbSnapshot) -> RaftStoreResult<()> {
        for cf_file in &mut self.cf_files {
            let tmp_path = format!("{}{}", cf_file.path.as_str().unwrap(), TMP_FILE_SUFFIX);
            if plain_file_used(cf_file.cf) {
                let f = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&tmp_path)?;
                cf_file.file = Some(f);
            } else {
                let handle = snap.cf_handle(cf_file.cf)?;
                let mut io_options = snap.get_db().get_options_cf(handle).clone();
                io_options.compression(get_fastest_supported_compression_type());
                // in rocksdb 5.5.1, SstFileWriter will try to use bottommost_compression and
                // compression_per_level first, so to make sure our specified compression type
                // being used, we must set them empty or disabled.
                io_options.compression_per_level(&[]);
                io_options.bottommost_compression(DBCompressionType::Disable);
                let mut writer = SstFileWriter::new(EnvOptions::new(), io_options);
                box_try!(writer.open(&tmp_path));
                cf_file.sst_writer = Some(writer);
            }
        }
        let meta_tmp_path = format!("{}{}", self.meta_file.path.as_str().unwrap(), TMP_FILE_SUFFIX);
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&meta_tmp_path)?;
        self.meta_file.file = Some(file);
        Ok(())
    }

    fn read_snapshot_meta(&self) -> Result<SnapshotMeta> {
    }

    fn set_snapshot_meta(&mut self, snapshot_meta: SnapshotMeta) -> RaftStoreResult<()> {
        if snapshot_meta.get_cf_files().len() != self.cf_files.len() {
            return Err(box_err!(
                "invalid cf number of snapshot meta, expect {}, got {}",
                SNAPSHOT_CFS.len(),
                snapshot_meta.get_cf_files().len()
            ));
        }
        for (i, cf_file) in self.cf_files.iter_mut().enumerate() {
            let meta = snapshot_meta.get_cf_files().get(i).unwrap();
            if meta.get_cf() != cf_file.cf {
                return Err(box_err!(
                    "invalid {} cf in snapshot meta, expect {}, got {}",
                    i,
                    cf_file.cf,
                    meta.get_cf()
                ));
            }
            if file_exists(&cf_file.path) {
                // Check only the file size for `exists()` to work correctly.
                check_file_size(&cf_file.path, meta.get_size())?;
            }
            cf_file.size = meta.get_size();
            cf_file.checksum = meta.get_checksum();
        }
        self.meta_file.meta = snapshot_meta;
        Ok(())
    }

    fn load_snapshot_meta(&mut self) -> RaftStoreResult<()> {
        let snapshot_meta = self.read_snapshot_meta()?;
        self.set_snapshot_meta(snapshot_meta)?;
        // check if there is a data corruption when the meta file exists
        // but cf files are deleted.
        if !self.exists() {
            return Err(box_err!(
                "snapshot {} is corrupted, some cf file is missing",
                self.path()
            ));
        }
        Ok(())
    }

    fn validate(&self, db: Arc<DB>) -> RaftStoreResult<()> {
        for cf_file in &self.cf_files {
            if cf_file.size == 0 {
                // Skip empty file. The checksum of this cf file should be 0 and
                // this is checked when loading the snapshot meta.
                continue;
            }
            if plain_file_used(cf_file.cf) {
                check_file_size_and_checksum(&cf_file.path, cf_file.size, cf_file.checksum)?;
            } else {
                prepare_sst_for_ingestion(&cf_file.path, &cf_file.clone_path)?;
                validate_sst_for_ingestion(
                    &db,
                    cf_file.cf,
                    &cf_file.clone_path,
                    cf_file.size,
                    cf_file.checksum,
                )?;
            }
        }
        Ok(())
    }

    fn switch_to_cf_file(&mut self, cf: &str) -> io::Result<()> {
        match self.cf_files.iter().position(|x| x.cf == cf) {
            Some(index) => {
                self.cf_index = index;
                Ok(())
            }
            None => Err(io::Error::new(
                ErrorKind::Other,
                format!("fail to find cf {}", cf),
            )),
        }
    }


    fn save_cf_files(&mut self) -> io::Result<()> {
        for cf_file in &mut self.cf_files {
            if plain_file_used(cf_file.cf) {
                let _ = cf_file.file.take();
            } else if cf_file.size == 0 {
                let _ = cf_file.sst_writer.take().unwrap();
            } else {
                let mut writer = cf_file.sst_writer.take().unwrap();
                if let Err(e) = writer.finish() {
                    return Err(io::Error::new(ErrorKind::Other, e));
                }
            }
            let size = get_file_size(&cf_file.tmp_path)?;
            if size > 0 {
                fs::rename(&cf_file.tmp_path, &cf_file.path)?;
                cf_file.size = size;
                // add size
                let mut size_track = self.size_track.wl();
                *size_track = size_track.saturating_add(size);

                cf_file.checksum = calc_crc32(&cf_file.path)?;
            } else {
                // Clean up the `tmp_path` if this cf file is empty.
                delete_file_if_exist(&cf_file.tmp_path);
            }
        }
        Ok(())
    }

    fn save_meta_file(&mut self) -> RaftStoreResult<()> {
        let mut v = vec![];
        box_try!(self.meta_file.meta.write_to_vec(&mut v));
        {
            let mut f = self.meta_file.file.take().unwrap();
            f.write_all(&v[..])?;
            f.flush()?;
        }
        fs::rename(&self.meta_file.tmp_path, &self.meta_file.path)?;
        Ok(())
    }

    fn do_build(&self, region: &Region, snap: &DbSnapshot) -> Result<usize> {
        let mut snap_key_count = 0;
        let (begin_key, end_key) = (enc_start_key(region), enc_end_key(region));
        for cf in SNAPSHOT_CFS {
            self.switch_to_cf_file(cf)?;
            let (cf_key_count, cf_size) = if plain_file_used(cf) {
                let file = self.cf_files[self.cf_index].file.as_mut().unwrap();
                build_plain_cf_file(file, snap, cf, &begin_key, &end_key)?
            } else {
                let writer = self.cf_files[self.cf_index].sst_writer.as_mut().unwrap();
                build_sst_cf_file(writer, snap, cf, &begin_key, &end_key)?;
            };
            snap_key_count += cf_key_count;
            SNAPSHOT_CF_KV_COUNT
                .with_label_values(&[cf])
                .observe(cf_key_count as f64);
            SNAPSHOT_CF_SIZE
                .with_label_values(&[cf])
                .observe(cf_size as f64);
            info!(
                "[region {}] scan snapshot {}, cf {}, key count {}, size {}",
                region.get_id(),
                self.path(),
                cf,
                cf_key_count,
                cf_size
            );
        }


        self.save_cf_files()?;
        self.meta_file.meta = gen_snapshot_meta(&self.cf_files)?;
        self.save_meta_file()?;
        Ok(snap_key_count)
    }
}

pub fn build_plain_cf_file<E: BytesEncoder>(
    encoder: &mut E,
    snap: &DbSnapshot,
    cf: &str,
    start_key: &[u8],
    end_key: &[u8],
) -> Result<(usize, usize)> {
    let mut cf_key_count = 0;
    let mut cf_size = 0;
    snap.scan_cf(cf, start_key, end_key, false, |key, value| {
        cf_key_count += 1;
        cf_size += key.len() + value.len();
        encoder.encode_compact_bytes(key)?;
        encoder.encode_compact_bytes(value)?;
        Ok(true)
    })?;
    // use an empty byte array to indicate that cf reaches an end.
    box_try!(encoder.encode_compact_bytes(b""));
    Ok((cf_key_count, cf_size))
}

pub fn build_sst_cf_file(sst_writer: &mut SstFileWriter, risnap: &DbSnapshot, cf: &str, start_key: &[u8], end_key: &[u8],
                         io_limiter: Option<&IOLimiter>) -> Result<(usize, usize)>
{
    let mut cf_key_count = 0;
    let mut cf_size = 0;
    let base = io_limiter.map_or(0i64, |l| l.get_max_bytes_per_time());
    let mut bytes: i64 = 0;
    snap.scan_cf(cf, start_key, end_key, false, |key, value| {
        let l = key.len() + value.len();
        cf_key_count += 1;
        cf_size += l;
        if let Some(io_limiter) = io_limiter {
            if bytes >= base {
                bytes = 0;
                self.snap_mgr_core.io_limiter.request(base);
            }
            bytes += l as i64;
        }
        sst_writer.put(key, value)?;
        Ok(true)
    })?;
    Ok((cf_key_count, cf_size))
}

fn apply_plain_cf_file<D: CompactBytesDecoder>(
    decoder: &mut D,
    options: &ApplyOptions,
    handle: &CFHandle,
) -> Result<()> {
    let mut wb = WriteBatch::new();
    let mut batch_size = 0;
    loop {
        check_abort(&options.abort)?;
        let key = box_try!(decoder.decode_compact_bytes());
        if key.is_empty() {
            if batch_size > 0 {
                box_try!(options.db.write(wb));
            }
            break;
        }
        box_try!(check_key_in_region(keys::origin_key(&key), &options.region));
        batch_size += key.len();
        let value = box_try!(decoder.decode_compact_bytes());
        batch_size += value.len();
        box_try!(wb.put_cf(handle, &key, &value));
        if batch_size >= options.write_batch_size {
            box_try!(options.db.write(wb));
            wb = WriteBatch::new();
            batch_size = 0;
        }
    }
    Ok(())
}

impl fmt::Debug for Snap {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("Snap")
            .field("key", &self.key)
            .field("display_path", &self.display_path)
            .finish()
    }
}

impl Snapshot for Snap {
    fn path(&self) -> &str {
        &self.display_path
    }

    fn total_size(&self) -> io::Result<u64> {
        Ok(self.cf_files.iter().fold(0, |acc, x| acc + x.size))
    }

    fn save(&mut self) -> io::Result<()> {
        debug!("saving to {}", self.path());
        for cf_file in &mut self.cf_files {
            if cf_file.size == 0 {
                // Skip empty cf file.
                continue;
            }

            // Check each cf file has been fully written, and the checksum matches.
            {
                let mut file = cf_file.file.take().unwrap();
                file.flush()?;
            }
            if cf_file.written_size != cf_file.size {
                return Err(io::Error::new(
                    ErrorKind::Other,
                    format!(
                        "snapshot file {} for cf {} size mismatches, \
                         real size {}, expected size {}",
                        cf_file.path.display(),
                        cf_file.cf,
                        cf_file.written_size,
                        cf_file.size
                    ),
                ));
            }
            let checksum = cf_file.write_digest.as_ref().unwrap().sum32();
            if checksum != cf_file.checksum {
                return Err(io::Error::new(
                    ErrorKind::Other,
                    format!(
                        "snapshot file {} for cf {} checksum \
                         mismatches, real checksum {}, expected \
                         checksum {}",
                        cf_file.path.display(),
                        cf_file.cf,
                        checksum,
                        cf_file.checksum
                    ),
                ));
            }

            fs::rename(&cf_file.tmp_path, &cf_file.path)?;
            let mut size_track = self.size_track.wl();
            *size_track = size_track.saturating_add(cf_file.size);
        }
        // write meta file
        let mut v = vec![];
        self.meta_file.meta.write_to_vec(&mut v)?;
        {
            let mut meta_file = self.meta_file.file.take().unwrap();
            meta_file.write_all(&v[..])?;
            meta_file.sync_all()?;
        }
        fs::rename(&self.meta_file.tmp_path, &self.meta_file.path)?;
        Ok(())
    }

    fn apply(&mut self, options: ApplyOptions) -> Result<()> {
        box_try!(self.validate(Arc::clone(&options.db)));

        for cf_file in &mut self.cf_files {
            if cf_file.size == 0 {
                // Skip empty cf file.
                continue;
            }

            check_abort(&options.abort)?;
            let cf_handle = box_try!(rocksdb::get_cf_handle(&options.db, cf_file.cf));
            if plain_file_used(cf_file.cf) {
                let mut file = box_try!(File::open(&cf_file.path));
                apply_plain_cf_file(&mut file, &options, cf_handle)?;
            } else {
                let mut ingest_opt = IngestExternalFileOptions::new();
                ingest_opt.move_files(true);
                let path = cf_file.clone_path.to_str().unwrap();
                box_try!(
                    options
                        .db
                        .ingest_external_file_cf(cf_handle, &ingest_opt, &[path])
                );
            }
        }
        Ok(())
    }
}

impl Read for Snap {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        while self.cf_index < self.cf_files.len() {
            let cf_file = &mut self.cf_files[self.cf_index];
            if cf_file.size == 0 {
                self.cf_index += 1;
                continue;
            }
            match cf_file.file.as_mut().unwrap().read(buf) {
                Ok(0) => {
                    // EOF. Switch to next file.
                    self.cf_index += 1;
                }
                Ok(n) => {
                    return Ok(n);
                }
                e => return e,
            }
        }
        Ok(0)
    }
}

impl Write for Snap {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        let mut next_buf = buf;
        while self.cf_index < self.cf_files.len() {
            let cf_file = &mut self.cf_files[self.cf_index];
            if cf_file.size == 0 {
                self.cf_index += 1;
                continue;
            }

            let left = (cf_file.size - cf_file.written_size) as usize;
            if left == 0 {
                self.cf_index += 1;
                continue;
            }

            let mut file = LimitWriter::new(self.limiter.clone(), cf_file.file.as_mut().unwrap());
            let digest = cf_file.write_digest.as_mut().unwrap();

            if next_buf.len() > left {
                file.write_all(&next_buf[0..left])?;
                digest.write(&next_buf[0..left]);
                cf_file.written_size += left as u64;
                self.cf_index += 1;
                next_buf = &next_buf[left..];
            } else {
                file.write_all(next_buf)?;
                digest.write(next_buf);
                cf_file.written_size += next_buf.len() as u64;
                return Ok(buf.len());
            }
        }
        let n = buf.len() - next_buf.len();
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        if let Some(cf_file) = self.cf_files.get_mut(self.cf_index) {
            let file = cf_file.file.as_mut().unwrap();
            file.flush()?;
        }
        Ok(())
    }
}

impl Drop for Snap {
    fn drop(&mut self) {
        // cleanup if some of the cf files and meta file is partly written
        if self.cf_files
            .iter()
            .any(|cf_file| file_exists(&cf_file.tmp_path))
            || file_exists(&self.meta_file.tmp_path)
        {
            self.delete();
            return;
        }
        // cleanup if data corruption happens and any file goes missing
        if !self.exists() {
            self.delete();
            return;
        }
    }
}

enum SnapEntry {
    Generating,
    Sending(AtomicUsize),
    Receiving,
    Applying,
}

/// `SnapStats` is for snapshot statistics.
pub struct SnapStats {
    pub sending_count: usize,
    pub receiving_count: usize,
}

fn notify_stats(ch: Option<&SendCh<Msg>>) {
    if let Some(ch) = ch {
        if let Err(e) = ch.try_send(Msg::SnapshotStats) {
            error!("notify snapshot stats failed {:?}", e)
        }
    }
}

struct SnapManagerCore {
    base: String,
    registry: RwLock<HashMap<SnapKey, SnapEntry>>,
    snap_size: AtomicUsize,
    io_limiter: Option<IOLimiter>,
}

impl SnapManagerCore {
    fn meta_file_path(&self, key: &SnapKey, for_sending: bool) -> impl Path {
        let meta_filename = if for_sending {
            format!("{}_{}{}", SNAP_GEN_PREFIX, key, META_FILE_SUFFIX)
        } else {
            format!("{}_{}", SNAP_REV_PREFIX, key, META_FILE_SUFFIX)
        };
        Path::new(&self.base).join(&meta_filename)
    }

    fn cf_file_path(&self, key: &SnapKey, cf: &str, for_sending: bool) -> impl Path {
        let cf_filename = if for_sending {
            format!("{}_{}_{}{}", SNAP_GEN_PREFIX, key, cf, SST_FILE_SUFFIX)
        } else {
            format!("{}_{}_{}{}", SNAP_REV_PREFIX, key, cf, SST_FILE_SUFFIX)
        }
        Path::new(&self.base).join(&cf_filename)
    }

    fn display_path(key: &SnapKey, for_sending: bool) -> String {
        let cf_names = "(".to_owned() + &SNAPSHOT_CFS.join("|") + ")";
        let prefix = if for_sending {
            SNAP_GEN_PREFIX
        } else {
            SNAP_REV_PREFIX
        };
        format!("{}/{}_{}{}", self.base, key, prefix, cf_names, SST_FILE_SUFFIX)
    }

}

#[derive(Clone)]
pub struct SnapManager {
    core: Arc<SnapManagerCore>,
    ch: Option<SendCh<Msg>>,
    limiter: Option<Arc<IOLimiter>>,
    max_total_size: u64,
}

impl SnapManager {
    pub fn new<T: Into<String>>(path: T, ch: Option<SendCh<Msg>>) -> io::Result<SnapManager> {
        let mgr = SnapManagerBuilder::default().build(path, ch);

        let path = Path::new(&mgr.core.base);
        if !path.exists() {
            fs::create_dir_all(path)?;
        }

        for f in fs::read_dir(path)? {
            let f = f?;
            if f.file_type()?.is_file() {
                if let Some(s) = f.file_name().to_str() {
                    if s.ends_with(TMP_FILE_SUFFIX) {
                        fs::remove_file(f.path())?;
                    } else if s.ends_with(SST_FILE_SUFFIX) {
                        mgr.core.snap_size.fetch_add(f.metadata()?.len())
                    }
                }
            }
        }
    }

    // Return all snapshots which is idle not being used.
    pub fn list_idle_snap(&self) -> io::Result<Vec<(SnapKey, bool)>> {
        let path = Path::new(&self.core.base);

        // Remove the duplicate snap keys.
        let mut v: Vec<_> = fs::read_dir(path)?
            .filter_map(|p| {
                let p = match p {
                    Err(e) => {
                        error!("failed to list content of {}: {:?}", self.core.base, e);
                        return None;
                    }
                    Ok(p) => p,
                };
                match p.file_type() {
                    Ok(t) if t.is_file() => {}
                    _ => return None,
                }
                let file_name = p.file_name();
                let name = match file_name.to_str() {
                    None => return None,
                    Some(n) => n,
                };
                let is_sending = name.starts_with(SNAP_GEN_PREFIX);
                let numbers: Vec<u64> = name.split('.').next().map_or_else(
                    || vec![],
                    |s| {
                        s.split('_')
                            .skip(1)
                            .filter_map(|s| s.parse().ok())
                            .collect()
                    },
                );
                if numbers.len() != 3 {
                    error!("failed to parse snapkey from {}", name);
                    return None;
                }
                let snap_key = SnapKey::new(numbers[0], numbers[1], numbers[2]);
                if self.core.registry.contains_key(&snap_key) {
                    // Skip those registered snapshot.
                    return None;
                }
                Some((snap_key, is_sending))
            })
            .collect();
        v.sort();
        v.dedup();
        Ok(v)
    }

    #[inline]
    pub fn has_registered(&self, key: &SnapKey) -> bool {
        self.core.registry.rl().contains_key(key)
    }

    pub fn build_snapshot_data(
        &self,
        key: &SnapKey,
        region: &Region,
        snap: &DbSnapshot,
    ) -> Result<RaftSnapshotData>> {
        let mut old_snaps = None;
        while self.get_total_snap_size() > self.max_total_snap_size() {
            if old_snaps.is_none() {
                let snaps = self.list_idle_snap()?;
                let mut key_and_modifies = Vec::with_capacity(snaps.len());
                for (key, _) in snaps.into_iter().filter(|&(_, is_sending)| is_sending) {
                    fs::metadata(&self.meta_file_path(key, true))
                        .and_then(|m| m.modified())
                        .map(|modified| key_and_modifies.push((key, modified)));
                }
                key_and_modifies.sort_by_key(|&(_, modified)| Reverse(modified));
                old_snaps = Some(key_and_modifies);
            }
            match old_snaps.as_mut().unwrap().pop() {
                Some((key, _)) => self.delete_snapshot(&key, true),
                None => return Err(RaftStoreError::Snapshot(Error::TooManySnapshots)),
            };
        }

        let s = Snap::new(key, self.core.clone());
        s.build_snapshot_data(region, snap)
    }

    pub fn get_snapshot_for_sending(&self, key: &SnapKey) -> RaftStoreResult<Box<Snapshot>> {
        let core = self.core.rl();
        let s = Snap::new_for_sending(
            &core.base,
            key,
            Arc::clone(&core.snap_size),
            Box::new(self.clone()),
        )?;
        Ok(Box::new(s))
    }

    pub fn get_snapshot_for_receiving(
        &self,
        key: &SnapKey,
        data: &[u8],
    ) -> RaftStoreResult<Box<Snapshot>> {
        let core = self.core.rl();
        let mut snapshot_data = RaftSnapshotData::new();
        snapshot_data.merge_from_bytes(data)?;
        let f = Snap::new_for_receiving(
            &core.base,
            key,
            snapshot_data.take_meta(),
            Arc::clone(&core.snap_size),
            Box::new(self.clone()),
            self.limiter.clone(),
        )?;
        Ok(Box::new(f))
    }

    pub fn get_snapshot_for_applying(&self, key: &SnapKey) -> RaftStoreResult<Box<Snapshot>> {
        let core = self.core.rl();
        let s = Snap::new_for_applying(
            &core.base,
            key,
            Arc::clone(&core.snap_size),
            Box::new(self.clone()),
        )?;
        if !s.exists() {
            return Err(RaftStoreError::Other(From::from(
                format!("snapshot of {:?} not exists.", key).to_string(),
            )));
        }
        Ok(Box::new(s))
    }

    /// Get the approximate size of snap file exists in snap directory.
    ///
    /// Return value is not guaranteed to be accurate.
    #[allow(let_and_return)]
    pub fn get_total_snap_size(&self) -> u64 {
        let core = self.core.rl();
        let size = *core.snap_size.rl();
        size
    }

    pub fn max_total_snap_size(&self) -> u64 {
        self.max_total_size
    }

    pub fn register(&self, key: SnapKey, entry: SnapEntry) {
        debug!("register [key: {}, entry: {:?}]", key, entry);
        let mut core = self.core.wl();
        match core.registry.entry(key) {
            Entry::Occupied(mut e) => {
                if e.get().contains(&entry) {
                    warn!("{} is registered more than 1 time!!!", e.key());
                    return;
                }
                e.get_mut().push(entry);
            }
            Entry::Vacant(e) => {
                e.insert(vec![entry]);
            }
        }

        notify_stats(self.ch.as_ref());
    }

    pub fn deregister(&self, key: &SnapKey, entry: &SnapEntry) {
        debug!("deregister [key: {}, entry: {:?}]", key, entry);
        let mut need_clean = false;
        let mut handled = false;
        let mut core = self.core.wl();
        if let Some(e) = core.registry.get_mut(key) {
            let last_len = e.len();
            e.retain(|e| e != entry);
            need_clean = e.is_empty();
            handled = last_len > e.len();
        }
        if need_clean {
            core.registry.remove(key);
        }
        if handled {
            notify_stats(self.ch.as_ref());
            return;
        }
        warn!("stale deregister key: {} {:?}", key, entry);
    }

    pub fn stats(&self) -> SnapStats {
        let core = self.core.rl();
        // send_count, generating_count, receiving_count, applying_count
        let (mut sending_cnt, mut receiving_cnt) = (0, 0);
        for v in core.registry.values() {
            let (mut is_sending, mut is_receiving) = (false, false);
            for s in v {
                match *s {
                    SnapEntry::Sending | SnapEntry::Generating => is_sending = true,
                    SnapEntry::Receiving | SnapEntry::Applying => is_receiving = true,
                }
            }
            if is_sending {
                sending_cnt += 1;
            }
            if is_receiving {
                receiving_cnt += 1;
            }
        }

        SnapStats {
            sending_count: sending_cnt,
            receiving_count: receiving_cnt,
        }
    }

    fn delete_snapshot(&self, key: &SnapKey, for_sending: bool) {
        // let core = self.core.rl();
        // if check_entry {
        //     if let Some(e) = core.registry.get(key) {
        //         if e.len() > 1 {
        //             info!(
        //                 "skip to delete {} since it's registered more than 1, registered \
        //                  entries {:?}",
        //                 snap.path(),
        //                 e
        //             );
        //             return false;
        //         }
        //     }
        // } else if core.registry.contains_key(key) {
        //     info!("skip to delete {} since it's registered", snap.path());
        //     return false;
        // }
        // true
        // TODO: impl it.
    }
}

#[derive(Debug, Default)]
pub struct SnapManagerBuilder {
    max_write_bytes_per_sec: u64,
    max_total_size: u64,
}

impl SnapManagerBuilder {
    pub fn max_write_bytes_per_sec(&mut self, bytes: u64) -> &mut SnapManagerBuilder {
        self.max_write_bytes_per_sec = bytes;
        self
    }
    pub fn max_total_size(&mut self, bytes: u64) -> &mut SnapManagerBuilder {
        self.max_total_size = bytes;
        self
    }
    pub fn build<T: Into<String>>(&self, path: T, ch: Option<SendCh<Msg>>) -> SnapManager {
        let limiter = if self.max_write_bytes_per_sec > 0 {
            Some(Arc::new(IOLimiter::new(self.max_write_bytes_per_sec)))
        } else {
            None
        };
        let max_total_size = if self.max_total_size > 0 {
            self.max_total_size
        } else {
            u64::MAX
        };
        SnapManager {
            core: Arc::new(SnapManagerCore {
                base: path.into(),
                registry: RwLock::new(map![]),
                snap_size: AtomicUsize::new(0),
            }),
            ch,
            limiter,
            max_total_size,
        }
    }
}
