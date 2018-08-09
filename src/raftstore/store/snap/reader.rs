use std::fs::File;
use std::io::{self, BufReader, Read};
use std::sync::atomic::{AtomicUsize, Ordering};

use kvproto::raft_serverpb::PeerState;
use rocksdb::rocksdb_options::WriteOptions;
use rocksdb::{IngestExternalFileOptions, Writable, WriteBatch};

use raftstore::store::engine::Mutable;
use raftstore::store::keys;
use raftstore::store::metrics::*;
use raftstore::store::snap::*;
use raftstore::store::util::check_key_in_region;
use storage::CF_RAFT;
use util::codec::bytes::CompactBytesFromFileDecoder;
use util::file::{calc_crc32, delete_file_if_exist, get_file_size};
use util::rocksdb::{get_cf_handle, prepare_sst_for_ingestion, validate_sst_for_ingestion};

struct SnapshotBase {
    dir: String,
    for_send: bool,
    key: SnapKey,
    snapshot_meta: SnapshotMeta,
}

impl SnapshotBase {
    fn new(dir: String, for_send: bool, key: SnapKey, snapshot_meta: SnapshotMeta) -> Self {
        SnapshotBase {
            dir,
            for_send,
            key,
            snapshot_meta,
        }
    }
}

pub struct SnapshotSender {
    base: SnapshotBase,
    snap_stale_notifier: Arc<SnapStaleNotifier>,
    ref_count: Arc<AtomicUsize>,
    sent_times: Arc<AtomicUsize>,

    cur_cf_file: Option<File>,
    cur_cf_pos: usize,
}

impl SnapshotSender {
    pub(super) fn new(
        dir: String,
        key: SnapKey,
        snapshot_meta: SnapshotMeta,
        snap_stale_notifier: Arc<SnapStaleNotifier>,
        ref_count: Arc<AtomicUsize>,
        sent_times: Arc<AtomicUsize>,
    ) -> Self {
        let base = SnapshotBase::new(dir, true, key, snapshot_meta);
        ref_count.fetch_add(1, Ordering::SeqCst);
        SnapshotSender {
            base,
            snap_stale_notifier,
            ref_count,
            sent_times,
            cur_cf_file: None,
            cur_cf_pos: 0,
        }
    }

    pub fn total_size(&self) -> u64 {
        get_size_from_snapshot_meta(&self.base.snapshot_meta)
    }
}

impl Drop for SnapshotSender {
    fn drop(&mut self) {
        self.ref_count.fetch_sub(1, Ordering::SeqCst);
        self.sent_times.fetch_add(1, Ordering::SeqCst);
    }
}

impl Read for SnapshotSender {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let base = &mut self.base;
        while self.cur_cf_pos < base.snapshot_meta.get_cf_files().len() {
            if stale_for_generate(base.key, self.snap_stale_notifier.as_ref()) {
                let err = io::Error::new(io::ErrorKind::Other, "stale snapshot".to_owned());
                return Err(err);
            }

            let cf = &base.snapshot_meta.get_cf_files()[self.cur_cf_pos];
            if cf.get_size() > 0 {
                if self.cur_cf_file.is_none() {
                    let (dir, for_send, key) = (&base.dir, base.for_send, base.key);
                    let cf_file_path = gen_cf_file_path(dir, for_send, key, cf.get_cf());
                    self.cur_cf_file = Some(File::open(&cf_file_path)?);
                }
                match self.cur_cf_file.as_mut().unwrap().read(buf) {
                    Ok(0) => {}
                    Ok(n) => return Ok(n),
                    e => return e,
                }
            }
            self.cur_cf_pos += 1;
            self.cur_cf_file = None;
        }
        Ok(0)
    }
}

pub(super) struct SnapshotApplyer {
    base: SnapshotBase,
    snap_stale_notifier: Arc<SnapStaleNotifier>,
    ref_count: Arc<AtomicUsize>,
    applied_times: Arc<AtomicUsize>,
}

impl SnapshotApplyer {
    pub(super) fn new(
        dir: String,
        key: SnapKey,
        snapshot_meta: SnapshotMeta,
        snap_stale_notifier: Arc<SnapStaleNotifier>,
        ref_count: Arc<AtomicUsize>,
        applied_times: Arc<AtomicUsize>,
    ) -> Result<Self> {
        let base = SnapshotBase::new(dir, false, key, snapshot_meta);
        if ref_count.compare_and_swap(0, 1, Ordering::SeqCst) != 0 {
            error!("{} apply_snapshot conflicts", key);
            return Err(Error::Snapshot(SnapError::Conflict));
        }
        Ok(SnapshotApplyer {
            base,
            snap_stale_notifier,
            ref_count,
            applied_times,
        })
    }

    pub(super) fn apply(&self, mut options: ApplyOptions) -> Result<()> {
        for cf in self.base.snapshot_meta.get_cf_files() {
            let (cf, size, checksum) = (cf.get_cf(), cf.get_size(), cf.get_checksum());
            if size == 0 {
                continue;
            }

            if plain_file_used(cf) {
                let path = gen_cf_file_path(&self.base.dir, false, self.base.key, cf);
                check_file_size_and_checksum(path, size, checksum)?;
                self.apply_plain_cf_file(cf, &options)?;
            } else {
                let (dir, key) = (&self.base.dir, self.base.key);
                let cf_path = gen_cf_file_path(dir, false, key, cf);
                let clone_path = gen_cf_clone_file_path(dir, false, key, cf);
                let db = options.db.as_ref();

                prepare_sst_for_ingestion(&cf_path, &clone_path)?;
                validate_sst_for_ingestion(db, cf, &clone_path, size, checksum)?;

                let mut options = IngestExternalFileOptions::new();
                options.move_files(true);
                let handle = box_try!(get_cf_handle(db, cf));
                let _timer = INGEST_SST_DURATION_SECONDS.start_coarse_timer();
                db.ingest_external_file_cf(handle, &options, &[clone_path.to_str().unwrap()])?;
            }
        }

        // Update region local state and snapshot raft state, so that
        // after apply finished, snap manager can delete snapshot safely.
        let handle = box_try!(get_cf_handle(&options.db, CF_RAFT));
        let wb = WriteBatch::new();

        let region_id = options.region_state.get_region().get_id();
        let region_key = keys::region_state_key(region_id);
        options.region_state.set_state(PeerState::Normal);
        box_try!(wb.put_msg_cf(handle, &region_key, &options.region_state));

        box_try!(wb.delete_cf(handle, &keys::snapshot_raft_state_key(region_id)));

        let mut write_opts = WriteOptions::new();
        write_opts.set_sync(true);
        box_try!(options.db.write_opt(wb, &write_opts));
        Ok(())
    }

    fn apply_plain_cf_file(&self, cf: &str, options: &ApplyOptions) -> Result<()> {
        let path = gen_cf_file_path(&self.base.dir, false, self.base.key, cf);
        let mut reader = BufReader::new(File::open(path)?);

        let handle = box_try!(get_cf_handle(&options.db, cf));
        let mut wb = WriteBatch::new();
        let mut finished = false;
        while !finished {
            if stale_for_apply(self.base.key, self.snap_stale_notifier.as_ref()) {
                return Err(Error::Snapshot(SnapError::Abort));
            }

            let key = reader.decode_compact_bytes()?;
            if key.is_empty() {
                finished = true;
            } else {
                let region = options.region_state.get_region();
                box_try!(check_key_in_region(keys::origin_key(&key), &region));
                let value = reader.decode_compact_bytes()?;
                box_try!(wb.put_cf(handle, &key, &value));
            }
            if wb.data_size() >= options.write_batch_size || finished {
                box_try!(options.db.write(wb));
                wb = WriteBatch::new();
            }
        }
        Ok(())
    }
}

impl Drop for SnapshotApplyer {
    fn drop(&mut self) {
        self.applied_times.fetch_add(1, Ordering::SeqCst);
        self.ref_count.fetch_sub(1, Ordering::SeqCst);

        for cf in self.base.snapshot_meta.get_cf_files() {
            if cf.get_size() == 0 || plain_file_used(cf.get_cf()) {
                continue;
            }

            let (dir, key) = (&self.base.dir, self.base.key);
            let clone_path = gen_cf_clone_file_path(dir, false, key, cf.get_cf());
            delete_file_if_exist(&clone_path);
        }
    }
}

fn gen_cf_clone_file_path(dir: &str, for_send: bool, key: SnapKey, cf: &str) -> PathBuf {
    let mut cf_path = gen_cf_file_path(dir, for_send, key, cf);
    let file_name = format!(
        "{}{}",
        cf_path.file_name().and_then(|n| n.to_str()).unwrap(),
        CLONE_FILE_SUFFIX,
    );
    cf_path.set_file_name(file_name);
    cf_path
}

fn check_file_size_and_checksum<P: AsRef<Path>>(
    path: P,
    expected_size: u64,
    expected_checksum: u32,
) -> Result<()> {
    let size = get_file_size(&path)?;
    if size != expected_size {
        return Err(snapshot_size_corrupt(expected_size, size));
    }

    let checksum = calc_crc32(&path)?;
    if checksum != expected_checksum {
        return Err(snapshot_checksum_corrupt(expected_checksum, checksum));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gen_cf_clone_file_path() {
        let key = SnapKey::new(1, 2, 3);
        for &(dir, for_send, key, cf, expected) in &[
            ("abc", true, key, CF_LOCK, "abc/gen_1_2_3/lock.sst.clone"),
            (
                "abc/",
                false,
                key,
                CF_WRITE,
                "abc/rev_1_2_3/write.sst.clone",
            ),
            (
                "ab/c",
                false,
                key,
                CF_DEFAULT,
                "ab/c/rev_1_2_3/default.sst.clone",
            ),
            ("", false, key, CF_LOCK, "rev_1_2_3/lock.sst.clone"),
        ] {
            let path = gen_cf_clone_file_path(dir, for_send, key, cf);
            assert_eq!(path.to_str().unwrap(), expected);
        }
    }
}
