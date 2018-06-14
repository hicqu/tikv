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

use std::boxed::FnBox;
use std::cmp::Reverse;
use std::fmt::{self, Display, Formatter};
use std::fs::{self, File, Metadata, OpenOptions};
use std::io::{self, BufReader, ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Instant;
use std::{error, result, str, thread, time, u64};

use crc::crc32::{self, Digest, Hasher32};
use protobuf::stream::CodedInputStream;
use protobuf::{Message, RepeatedField};

use kvproto::metapb::Region;
use kvproto::raft_serverpb::{RaftSnapshotData, SnapshotCFFile, SnapshotMeta};
use raft::eraftpb::Snapshot as RaftSnapshot;
use rocksdb::{CFHandle, Writable, WriteBatch, DB};
use rocksdb::{DBCompressionType, EnvOptions, IngestExternalFileOptions, SstFileWriter};

use raftstore::store::Msg;
use raftstore::store::engine::{Iterable, Snapshot as DbSnapshot};
use raftstore::store::keys::{self, enc_end_key, enc_start_key};
use raftstore::store::metrics::*;
use raftstore::store::peer_storage::JOB_STATUS_CANCELLING;
use raftstore::store::util::check_key_in_region;
use raftstore::{Error as RaftStoreError, Result as RaftStoreResult};
use storage::{CfName, CF_DEFAULT, CF_LOCK, CF_WRITE};
use util::codec::bytes::{BytesEncoder, CompactBytesFromFileDecoder};
use util::collections::{HashMap, HashMapEntry as Entry};
use util::file::{delete_file_if_exist, file_exists, get_file_size, calc_crc32};
use util::io_limiter::{IOLimiter, LimitWriter};
use util::rocksdb::{get_fastest_supported_compression_type, prepare_sst_for_ingestion,
                    validate_sst_for_ingestion};
use util::time::duration_to_sec;
use util::transport::SendCh;

const SNAPSHOT_VERSION: u64 = 2;
const SNAPSHOT_CFS: &[CfName] = &[CF_DEFAULT, CF_LOCK, CF_WRITE];

const SNAP_GEN_PREFIX: &str = "gen";
const SNAP_REV_PREFIX: &str = "rev";

const META_FILE_SUFFIX: &str = ".meta";
const SST_FILE_SUFFIX: &str = ".sst";
const TMP_FILE_SUFFIX: &str = ".tmp";
const CLONE_FILE_SUFFIX: &str = ".clone";

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Abort {
            description("abort")
            display("abort")
        }
        Conflict(registered: &'static str, new: &'static str) {
            description("want to register {}, but {} exists", new, registered)
            display("Register Conflict")
        }
        Other(err: Box<error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("snap failed {:?}", err)
        }
    }
}

#[derive(Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Debug)]
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

#[derive(Default)]
struct CfFile {
    cf: CfName,
    path: PathBuf,
    sst_writer: Option<SstFileWriter>,
    file: Option<File>,
    write_digest: Option<Digest>,
}

impl CfFile {
    fn new(dir_path: &PathBuf, prefix: &str, cf: CfName) -> Self {
        let file_name = format!("{}_{}{}", prefix, cf, SST_FILE_SUFFIX);
        let path = dir_path.join(&file_name);
        CfFile {
            cf,
            path,
            ..Default::default()
        }
    }

    fn plain_file_used(&self) -> bool {
        self.cf == CF_LOCK
    }

    fn tmp_path(&self) -> PathBuf {
        let file_name = self.path.file_name().and_then(|n| n.to_str()).unwrap();
        let file_name = format!("{}{}", file_name, TMP_FILE_SUFFIX);
        let tmp_path = self.path.clone();
        tmp_path.set_file_name(file_name);
        tmp_path
    }
}

#[derive(Default)]
struct MetaFile {
    meta: SnapshotMeta,
    path: PathBuf,
    file: Option<File>,
}

impl MetaFile {
    fn new(dir_path: &PathBuf, prefix: &str) -> Self {
        let file_name = format!("{}{}", prefix, META_FILE_SUFFIX);
        MetaFile {
            path: dir_path.join(&file_name),
            ..Default::default()
        }
    }

    fn tmp_path(&self) -> PathBuf {
        let file_name = self.path.file_name().and_then(|n| n.to_str()).unwrap();
        let file_name = format!("{}{}", file_name, TMP_FILE_SUFFIX);
        let tmp_path = self.path.clone();
        tmp_path.set_file_name(file_name);
        tmp_path
    }
}

pub struct Snap {
    key: SnapKey,
    display_path: String,
    meta_file: MetaFile,
    cf_files: Vec<CfFile>,
    hold_tmp_files: bool,
    // Internal status for implement Read and Write.
    cf_index: usize,
    // Callback to update status in registry.
    deregister: Option<Box<FnBox() + Send>>,
}

impl Snap {
    fn get_display_path(dir_path: &PathBuf, prefix: &str) -> String {
        let cf_names = "(".to_owned() + &SNAPSHOT_CFS.join("|") + ")";
        format!(
            "{}/{}_{}{}",
            dir_path.display(),
            prefix,
            cf_names,
            SST_FILE_SUFFIX
        )
    }

    fn new(dir_path: PathBuf, key: SnapKey, is_sending: bool) -> Self {
        let prefix = if is_sending {
            format!("{}_{}", SNAP_GEN_PREFIX, key)
        } else {
            format!("{}_{}", SNAP_REV_PREFIX, key)
        };
        let display_path = Snap::get_display_path(&dir_path, &prefix);

        let cf_files = SNAPSHOT_CFS
            .iter()
            .map(|cf| CfFile::new(&dir_path, &prefix, cf))
            .collect();
        let meta_file = MetaFile::new(&dir_path, &prefix);

        Snap {
            key: key.clone(),
            display_path,
            meta_file,
            cf_files,
            hold_tmp_files: false,
            cf_index: 0,
            deregister: None,
        }
    }

    // Load the snapshot from disk without check size and checksum of cf files.
    fn load(&mut self) -> RaftStoreResult<()> {
        let mut file = File::open(&self.meta_file.path)?;
        let mut stream = CodedInputStream::new(&mut file);
        self.meta_file.meta.merge_from(&mut stream)?;
        Ok(())
    }

    fn init_for_sending(&mut self) -> RaftStoreResult<()> {
        self.meta_file.file = Some(File::open(&self.meta_file.path)?);
        for cf_file in &mut self.cf_files {
            cf_file.file = Some(File::open(&cf_file.path)?);
        }
        Ok(())
    }

    fn init_for_receiving(&mut self) -> RaftStoreResult<()> {
        let open_options = OpenOptions::new().write(true).create_new(true);
        self.meta_file.file = Some(open_options.open(&self.meta_file.tmp_path())?);
        self.hold_tmp_files = true;
        for cf_file in &mut self.cf_files {
            cf_file.file = Some(open_options.open(&cf_file.tmp_path())?);
            cf_file.write_digest = Some(Digest::new(crc32::IEEE));
        }
        Ok(())
    }

    fn prepare_tmp_files(&mut self, db_snap: &DbSnapshot) -> RaftStoreResult<()> {
        let open_options = OpenOptions::new().write(true).create_new(true);
        self.meta_file.file = Some(open_options.open(&self.meta_file.tmp_path())?);
        self.hold_tmp_files = true;

        for cf_file in &mut self.cf_files {
            if cf_file.plain_file_used() {
                cf_file.file = Some(open_options.open(&cf_file.tmp_path())?);
            } else {
                let handle = db_snap.cf_handle(cf_file.cf)?;
                let mut io_options = db_snap.get_db().get_options_cf(handle).clone();
                io_options.compression(get_fastest_supported_compression_type());
                // in rocksdb 5.5.1, SstFileWriter will try to use bottommost_compression and
                // compression_per_level first, so to make sure our specified compression type
                // being used, we must set them empty or disabled.
                io_options.compression_per_level(&[]);
                io_options.bottommost_compression(DBCompressionType::Disable);
                let mut writer = SstFileWriter::new(EnvOptions::new(), io_options);
                box_try!(writer.open(cf_file.tmp_path().as_path().to_str().unwrap()));
                cf_file.sst_writer = Some(writer);
            }
        }
        Ok(())
    }

    // Rename tmp files to sst files.
    fn rename_tmp_files(&mut self) -> RaftStoreResult<()> {
        for cf_file in &mut self.cf_files {
            if !cf_file.plain_file_used() {
                let writer = cf_file.sst_writer.as_mut().unwrap();
                writer.finish()?;
            }

            let tmp_path = cf_file.tmp_path();
            let checksum = calc_crc32(&tmp_path)?;
            let file_size = get_file_size(&tmp_path)?;

            let mut snap_cf_file = SnapshotCFFile::new();
            snap_cf_file.set_cf(cf_file.cf.to_owned());
            snap_cf_file.set_size(file_size);
            snap_cf_file.set_checksum(checksum);
            self.meta_file.meta.mut_cf_files().push(snap_cf_file);

            if file_size > 0 {
                fs::rename(&tmp_path, &cf_file.path)?;
            }
            drop(cf_file.file.take());
            drop(cf_file.sst_writer.take());
        }
        let mut file = self.meta_file.file.take().unwrap();
        self.meta_file.meta.write_to_writer(&mut file)?;
        fs::rename(&self.meta_file.tmp_path(), &self.meta_file.path)?;
        self.hold_tmp_files = false;
        Ok(())
    }

    // Build the snapshot, write data into tmp files.
    fn build(
        &mut self,
        region: &Region,
        db_snap: &DbSnapshot,
        limiter: Option<&IOLimiter>,
    ) -> RaftStoreResult<()> {
        let t = Instant::now();
        let (start_key, end_key) = (enc_start_key(region), enc_end_key(region));
        let mut snap_key_count = 0;
        self.prepare_tmp_files(db_snap)?;
        for cf_file in &mut self.cf_files {
            let (mut cf_key_count, mut cf_size) = (0, 0);
            if cf_file.plain_file_used() {
                let file = cf_file.file.as_mut().unwrap();
                db_snap.scan_cf(cf_file.cf, &start_key, &end_key, false, |key, value| {
                    cf_key_count += 1;
                    cf_size += key.len() + value.len();
                    file.encode_compact_bytes(key)?;
                    file.encode_compact_bytes(value)?;
                    Ok(true)
                })?;
                // use an empty byte array to indicate that cf reaches an end.
                file.encode_compact_bytes(b"")?;
            } else {
                let writer = cf_file.sst_writer.as_mut().unwrap();
                let base = limiter
                    .as_ref()
                    .map_or(0 as i64, |l| l.get_max_bytes_per_time());
                let mut bytes = 0;
                db_snap.scan_cf(cf_file.cf, &start_key, &end_key, false, |key, value| {
                    let l = key.len() + value.len();
                    cf_key_count += 1;
                    cf_size += l;
                    if let Some(ref limiter) = limiter {
                        if bytes >= base {
                            bytes = 0;
                            limiter.request(base);
                        }
                        bytes += l as i64;
                    }
                    writer.put(key, value)?;
                    Ok(true)
                })?;
            }

            snap_key_count += cf_key_count;
            SNAPSHOT_CF_KV_COUNT
                .with_label_values(&[cf_file.cf])
                .observe(cf_key_count as f64);
            SNAPSHOT_CF_SIZE
                .with_label_values(&[cf_file.cf])
                .observe(cf_size as f64);
            info!(
                "[region {}] scan snapshot {}, cf {}, key count {}, size {}",
                region.get_id(),
                self.display_path,
                cf_file.cf,
                cf_key_count,
                cf_size
            );
        }

        self.rename_tmp_files()?;
        let total_size = self.total_size();

        SNAPSHOT_BUILD_TIME_HISTOGRAM.observe(duration_to_sec(t.elapsed()) as f64);
        SNAPSHOT_KV_COUNT_HISTOGRAM.observe(snap_key_count as f64);
        SNAPSHOT_SIZE_HISTOGRAM.observe(total_size as f64);
        info!(
            "[region {}] scan snapshot {}, size {}, key count {}, takes {:?}",
            region.get_id(),
            self.display_path,
            total_size,
            snap_key_count,
            t.elapsed(),
        );
        Ok(())
    }

    pub fn path(&self) -> &str {
        &self.display_path
    }

    pub fn total_size(&self) -> u64 {
        self.meta_file
            .meta
            .get_cf_files()
            .iter()
            .fold(0, |acc, x| acc + x.get_size() as u64)
    }
}

impl Read for Snap {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        while self.cf_index < self.cf_files.len() {
            let cf_size = self.meta_file.meta.get_cf_files()[self.cf_index].get_size();
            if cf_size == 0 {
                self.cf_index += 1;
                continue;
            }

            let file = self.cf_files[self.cf_index].file.as_mut().unwrap();
            match file.read(buf) {
                Ok(0) => self.cf_index += 1,
                Ok(n) => return Ok(n),
                e => return e,
            }
        }
        Ok(0)
    }
}

impl Drop for Snap {
    fn drop(&mut self) {
        if !self.hold_tmp_files {
            return;
        }
        for cf in &mut self.cf_files {
            if cf.file.is_some() || cf.sst_writer.is_some() {
                delete_file_if_exist(&cf.tmp_path());
            }
        }
        if self.meta_file.file.is_some() {
            delete_file_if_exist(&self.meta_file.tmp_path());
        }
        if let Some(deregister) = self.deregister.take() {
            deregister();
        }
    }
}

impl fmt::Debug for Snap {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("Snap")
            .field("key", &self.key)
            .field("display_path", &self.display_path)
            .finish()
    }
}

/// `SnapStats` is for snapshot statistics.
#[derive(Default)]
pub struct SnapStats {
    pub sending_count: usize,
    pub receiving_count: usize,
}

#[derive(Debug, Eq, PartialEq)]
enum SnapEntry {
    Generating,
    Generated,
    Sending(usize),
    Receiving,
    Received,
    Applying,
}

impl SnapEntry {
    fn display(&self) -> &'static str {
        match *self {
            SnapEntry::Generating => "Generating",
            SnapEntry::Generated => "Generated",
            SnapEnry::Sending(_) => "Sending",
            SnapEnry::Receiving => "Receiving",
            SnapEnry::Received => "Received",
            SnapEnry::Applying => "Applying",
        }
    }
}

struct SnapManagerCore {
    base: String,
    registry: Mutex<HashMap<SnapKey, SnapEntry>>,
    snap_size: AtomicU64,
    limiter: Option<IOLimiter>,
}

impl SnapManagerCore {
    // Return a tuple indicates (redundant, need_build).
    fn before_build(&self, key: SnapKey) -> (bool, bool) {
        match self.registry.lock().unwrap().entry(key) {
            Entry::Occupied(e) => match e.get() {
                SnapEntry::Generating => (true, false),
                SnapEntry::Generated | SnapEntry::Sending(_) => (false, false),
                _ => unreachable!(),
            },
            Entry::Vacant(e) => {
                e.insert(SnapEntry::Generating);
                (true, true)
            }
        }
    }

    fn post_build_success(&self, key: SnapKey) {
        match self.registry.lock().unwrap().entry(key) {
            Entry::Occupied(e) => {
                let e_mut = e.get_mut();
                assert_eq!(e_mut, &SnapEntry::Generating);
                *e_mut = SnapEntry::Generated;
            }
            _ => unreachable!(),
        }
    }

    fn post_build_fail(&self, key: SnapKey) {
        match self.registry.lock().unwrap().entry(key) {
            Entry::Occupied(e) => {
                assert_eq!(e.get(), &SnapEntry::Generating);
                e.remove();
            }
            _ => unreachable!(),
        }
    }

    fn before_send(&self, key: SnapKey) {
        match self.registry.lock().unwrap().entry(key) {
            Entry::Occupied(e) => {
                let e_mut = e.get_mut();
                match e_mut {
                    SnapEntry::Generated => *e_mut = SnapEntry::Sending(1),
                    SnapEntry::Sending(c) => *c += 1,
                    _ => unreachable!(),
                };
            }
            _ => unreachable!(),
        }
    }

    fn post_send(&self, key: SnapKey) {
        match self.registry.lock().unwrap().entry(key) {
            Entry::Occupied(e) => {
                let e_mut = e.get_mut();
                match e_mut {
                    SnapEntry::Sending(c) => {
                        *c -= 1;
                        if *c == 0 {
                            *e_mut = SnapEntry::Generated;
                        }
                    }
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
    }

    fn before_receiving(&self, key: SnapKey) -> ::std::result::Result<Option<()>, Error> {
        match self.registry.lock().unwrap().entry(key) {
            Entry::Occupied(e) => {
                let e = e.get();
                match e {
                    SnapEntry::Received => return Ok(None),
                    e => return Error::Config(e.display(), "Receiving"),
                }
            }
            Entry::Vacant(e) => {
                e.insert(SnapEntry::Receiving);
                return Ok(Some(()));
            }
        }
    }

    fn post_receiving(&self, key: SnapKey) {
        // TODO: finish this.
        // have too much redundant code.
    }
}

#[derive(Clone)]
pub struct SnapManager {
    core: Arc<SnapManagerCore>,
    ch: Option<SendCh<Msg>>,
}

impl SnapManager {
    pub fn new<T: Into<String>>(path: T, ch: Option<SendCh<Msg>>) -> SnapManager {
        SnapManagerBuilder::default().build(path, ch)
    }

    pub fn init(&self) -> io::Result<()> {
        let path = Path::new(&self.core.base);
        fs::create_dir_all(path)?;
        for p in fs::read_dir(path)?.map(|f| f.unwrap()) {
            if p.file_type()?.is_file() {
                if let Some(s) = p.file_name().to_str() {
                    if s.ends_with(TMP_FILE_SUFFIX) {
                        fs::remove_file(p.path())?;
                    } else if s.ends_with(SST_FILE_SUFFIX) {
                        let len = p.metadata()?.len();
                        self.core.snap_size.fetch_add(len, Ordering::SeqCst);
                    }
                }
            }
        }
        Ok(())
    }

    /// Build a snapshot with metadata in `RaftSnapshotData`.
    /// To get the readable snapshot, call `get_snapshot_for_sending`.
    pub fn build_snapshot(
        &self,
        key: SnapKey,
        region: &Region,
        db_snap: &DbSnapshot,
    ) -> RaftStoreResult<Option<RaftSnapshotData>> {
        let (redundant, need_build) = self.core.before_build(key);
        if redundant {
            return Ok(None);
        }

        let mut snap = Snap::new(PathBuf::from(&self.core.base), key, true);
        if need_build {
            let limiter = self.core.limiter.as_ref();
            let core = Arc::clone(&self.core);
            match snap.build(region, db_snap, limiter) {
                Ok(_) => {
                    core.snap_size.fetch_add(total_size, Ordering::SeqCst);
                    snap.deregister = Some(box move || core.post_build_success(key));
                }
                Err(e) => {
                    snap.deregister = Some(box move || core.post_build_fail(key));
                    return Err(e);
                }
            };
        } else {
            snap.load()?;
        }

        let mut snapshot_data = RaftSnapshotData::new();
        snapshot_data.set_file_size(snap.total_size() as u64);
        snapshot_data.set_version(SNAPSHOT_VERSION);
        snapshot_data.set_meta(snap.meta_file.meta);
        Ok(Some(snapshot_data))
    }

    /// Get a snapshot so that we can read bytes from it.
    pub fn get_snapshot_for_sending(&self, key: SnapKey) -> RaftStoreResult<Snap> {
        let mut snap = Snap::new(PathBuf::from(&self.core.base), key, true);
        snap.init_for_sending()?;

        self.core.before_send(key);
        let core = Arc::clone(&self.core);
        snap.deregister = Some(box move || core.post_send(key));

        Ok(snap)
    }

    pub fn get_snapshot_for_receiving(
        &self,
        key: SnapKey,
        data: &[u8],
    ) -> RaftStoreResult<Option<Snap>> {
        let mut snap = Snap::new(PathBuf::from(&self.core.base), key, false);
        snap.meta_file.meta.merge_from_bytes(data)?;
        snap.init_for_receiving()?;

        match self.core.before_receiving(key)? {
            Ok(Some(_)) => {
                let core = Arc::clone(&self.core);
                snap.deregister = Some(box move || core.post_receiving(key));
                Ok(Some(snap))
            }
            err_or_none => return err_or_none,
        }
    }

    pub fn get_snapshot_for_applying(&self, key: SnapKey) -> RaftStoreResult<Snap> {
        // TODO: finish this.
        let mut snap = Snap::new(PathBuf::from(&self.core.base), key, false);
        Ok(snap)
    }

    pub fn delete_snapshot(&self, key: SnapKey) {}

    pub fn get_total_snap_size(&self) -> u64 {
        self.core.snap_size.load(Ordering::SeqCst)
    }

    pub fn stats(&self) -> SnapStats {
        let mut stats = SnapStats::default();
        for entry in self.core.registry.lock().unwrap().values() {
            match entry {
                SnapEntry::Sending(c) => stats.sending_count += *c,
                SnapEntry::Received => stats.receiving_count += 1,
            }
        }
        stats
    }
}

#[derive(Debug, Default)]
pub struct SnapManagerBuilder {
    max_write_bytes_per_sec: u64,
}

impl SnapManagerBuilder {
    pub fn max_write_bytes_per_sec(&mut self, bytes: u64) -> &mut SnapManagerBuilder {
        self.max_write_bytes_per_sec = bytes;
        self
    }

    pub fn build<T: Into<String>>(&self, path: T, ch: Option<SendCh<Msg>>) -> SnapManager {
        let limiter = if self.max_write_bytes_per_sec > 0 {
            Some(IOLimiter::new(self.max_write_bytes_per_sec))
        } else {
            None
        };
        SnapManager {
            core: Arc::new(SnapManagerCore {
                base: path.into(),
                registry: Mutex::new(map![]),
                snap_size: AtomicU64::new(0),
                limiter: limiter,
            }),
            ch,
        }
    }
}
