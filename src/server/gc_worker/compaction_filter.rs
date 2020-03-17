// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp::Ordering as CmpOrdering;
use std::ffi::CString;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use super::GcWorkerConfigManager;
use crate::storage::mvcc::{GC_DELETE_VERSIONS_HISTOGRAM, MVCC_VERSIONS_HISTOGRAM};
use engine::rocks::{
    new_compaction_filter_raw, CFHandle, CompactionFilter, CompactionFilterContext,
    CompactionFilterFactory, DBCompactionFilter, DBIterator, SeekKey, Writable, WriteOptions, DB,
};
use engine_rocks::{util as rocks_util, RocksWriteBatch};
use engine_traits::{WriteBatch, CF_DEFAULT, CF_GC};
use txn_types::{Key, WriteRef, WriteType};

const DEFAULT_DELETE_BATCH_SIZE: usize = 256 * 1024;

struct GcContext {
    db: Arc<DB>,
    safe_point: Arc<AtomicU64>,
    cfg_tracker: GcWorkerConfigManager,
}

lazy_static! {
    static ref GC_CONTEXT: Mutex<Option<GcContext>> = Mutex::new(None);
}

pub fn init_compaction_filter(
    db: Arc<DB>,
    safe_point: Arc<AtomicU64>,
    cfg_tracker: GcWorkerConfigManager,
) {
    info!("initialize GC context for compaction filter");
    let mut gc_context = GC_CONTEXT.lock().unwrap();
    *gc_context = Some(GcContext {
        db,
        safe_point,
        cfg_tracker,
    });
}

pub struct WriteCompactionFilterFactory;

impl CompactionFilterFactory for WriteCompactionFilterFactory {
    fn create_compaction_filter(
        &self,
        _context: &CompactionFilterContext,
    ) -> *mut DBCompactionFilter {
        let gc_context_option = GC_CONTEXT.lock().unwrap();
        let gc_context = match *gc_context_option {
            Some(ref ctx) => ctx,
            None => return std::ptr::null_mut(),
        };
        if !gc_context.cfg_tracker.value().enable_compaction_filter {
            return std::ptr::null_mut();
        }
        if gc_context.safe_point.load(Ordering::Relaxed) == 0 {
            // Safe point has not been initialized yet.
            return std::ptr::null_mut();
        }

        let name = CString::new("write_compaction_filter").unwrap();
        let db = Arc::clone(&gc_context.db);
        let safe_point = Arc::clone(&gc_context.safe_point);

        let filter = Box::new(WriteCompactionFilter::new(db, safe_point));
        unsafe { new_compaction_filter_raw(name, filter) }
    }
}

struct WriteCompactionFilter {
    safe_point: Arc<AtomicU64>,
    db: Arc<DB>,

    cf_handle: &'static CFHandle,
    write_batch: RocksWriteBatch,
    key_prefix: Vec<u8>,
    remove_older: bool,

    versions: usize,
    deleted: usize,
}

impl WriteCompactionFilter {
    fn new(db: Arc<DB>, safe_point: Arc<AtomicU64>) -> Self {
        // Safe point must have been initialized.
        assert!(safe_point.load(Ordering::Relaxed) > 0);

        let db_ref: &'static DB = unsafe { std::mem::transmute(db.as_ref()) };
        let cf_handle = rocks_util::get_cf_handle(db_ref, CF_GC).unwrap();
        let wb = RocksWriteBatch::with_capacity(Arc::clone(&db), DEFAULT_DELETE_BATCH_SIZE);
        WriteCompactionFilter {
            safe_point,
            db,
            cf_handle,
            write_batch: wb,
            key_prefix: vec![],
            remove_older: false,

            versions: 0,
            deleted: 0,
        }
    }

    fn delete_default_key(&mut self, key: &[u8]) {
        self.write_batch
            .as_inner()
            .put_cf(&self.cf_handle, key, b"")
            .unwrap();
        if self.write_batch.data_size() > DEFAULT_DELETE_BATCH_SIZE {
            let mut opts = WriteOptions::new();
            opts.set_sync(false);
            self.db
                .write_opt(self.write_batch.as_inner(), &opts)
                .unwrap();
            self.write_batch.clear();
        }
    }

    fn reset_statistics(&mut self) {
        if self.versions != 0 {
            MVCC_VERSIONS_HISTOGRAM.observe(self.versions as f64);
            self.versions = 0;
        }
        if self.deleted != 0 {
            GC_DELETE_VERSIONS_HISTOGRAM.observe(self.deleted as f64);
            self.deleted = 0;
        }
    }
}

impl Drop for WriteCompactionFilter {
    fn drop(&mut self) {
        if !self.write_batch.is_empty() {
            let mut opts = WriteOptions::new();
            opts.set_sync(true);
            self.db
                .write_opt(self.write_batch.as_inner(), &opts)
                .unwrap();
        } else {
            self.db.flush(true).unwrap();
        }
    }
}

impl CompactionFilter for WriteCompactionFilter {
    fn filter(
        &mut self,
        _level: usize,
        key: &[u8],
        value: &[u8],
        _: &mut Vec<u8>,
        _: &mut bool,
    ) -> bool {
        let safe_point = self.safe_point.load(Ordering::Relaxed);
        let (key_prefix, commit_ts) = match Key::split_on_ts_for(key) {
            Ok((key, ts)) => (key, ts),
            // Invalid MVCC keys, don't touch them.
            Err(_) => return false,
        };

        if self.key_prefix != key_prefix {
            self.key_prefix.clear();
            self.key_prefix.extend_from_slice(key_prefix);
            self.remove_older = false;
            self.reset_statistics();
        }

        self.versions += 1;
        if commit_ts.into_inner() > safe_point {
            return false;
        }

        let mut filtered = self.remove_older;
        let WriteRef {
            write_type,
            start_ts,
            short_value,
        } = WriteRef::parse(value).unwrap();
        if !self.remove_older {
            // here `filtered` must be false.
            match write_type {
                WriteType::Rollback | WriteType::Lock => filtered = true,
                WriteType::Delete => {
                    // Currently `WriteType::Delete` will always be kept.
                    self.remove_older = true;
                }
                WriteType::Put => self.remove_older = true,
            }
        }

        if filtered {
            if short_value.is_none() {
                let key = Key::from_encoded_slice(key_prefix).append_ts(start_ts);
                self.delete_default_key(key.as_encoded());
            }
            self.deleted += 1;
        }

        filtered
    }
}

pub struct DefaultCompactionFilterFactory;

impl CompactionFilterFactory for DefaultCompactionFilterFactory {
    fn create_compaction_filter(
        &self,
        _context: &CompactionFilterContext,
    ) -> *mut DBCompactionFilter {
        if let Some((db, safe_point)) = need_filter_gc_table() {
            let name = CString::new("default_compaction_filter").unwrap();
            // TODO: Carry `start_key` and `end_key` in `CompactionFilterContext` to improve
            // iterating on the GC table.
            let filter = Box::new(DefaultCompactionFilter::new(db, safe_point));
            return unsafe { new_compaction_filter_raw(name, filter) };
        }
        std::ptr::null_mut()
    }
}

struct DefaultCompactionFilter {
    safe_point: Arc<AtomicU64>,
    db: Arc<DB>,
    iter: Option<DBIterator<&'static DB>>,
    no_more: bool,
    deleted: usize,
}

impl DefaultCompactionFilter {
    fn new(db: Arc<DB>, safe_point: Arc<AtomicU64>) -> Self {
        DefaultCompactionFilter {
            safe_point,
            db,
            iter: None,
            no_more: false,
            deleted: 0,
        }
    }

    fn next(&mut self) -> bool {
        let valid = self.iter.as_mut().unwrap().next().unwrap();
        if !valid {
            self.no_more = true;
            self.iter = None;
        }
        valid
    }
}

impl CompactionFilter for DefaultCompactionFilter {
    fn filter(
        &mut self,
        _level: usize,
        key: &[u8],
        _value: &[u8],
        _: &mut Vec<u8>,
        _: &mut bool,
    ) -> bool {
        if self.no_more {
            // There are no more stale versions in the GC table.
            return false;
        }

        let (_, start_ts) = match Key::split_on_ts_for(key) {
            Ok((key, ts)) => (key, ts),
            // Invalid MVCC keys, don't touch them.
            Err(_) => return false,
        };

        let safe_point = self.safe_point.load(Ordering::Relaxed);
        if safe_point > 0 && start_ts.into_inner() > safe_point {
            // The transaction is still valid.
            return false;
        }

        if self.iter.is_none() {
            let db: &'static DB = unsafe { std::mem::transmute(self.db.as_ref()) };
            let cf_handle = rocks_util::get_cf_handle(db, CF_GC).unwrap();
            let mut iter = db.iter_cf(cf_handle);
            if !iter.seek(SeekKey::Key(key)).unwrap() {
                // Can't seek to the position, which means no more stale versions.
                self.no_more = true;
                return false;
            }
            self.iter = Some(iter);
        }

        loop {
            match self.iter.as_ref().unwrap().key().cmp(key) {
                // NOTE: if the gc table can't be cleared promptly, we could call the `next` many
                // times. We can consider to call `seek` after some `next`s. Or, we can put deleted
                // keys into a SST file, and then integrate it into the gc table.
                CmpOrdering::Less if !self.next() => return false,
                CmpOrdering::Greater => return false,
                CmpOrdering::Equal => {
                    self.deleted += 1;
                    return true;
                }
                _ => {}
            }
        }
    }
}

pub struct GcCompactionFilterFactory;

impl CompactionFilterFactory for GcCompactionFilterFactory {
    fn create_compaction_filter(
        &self,
        _context: &CompactionFilterContext,
    ) -> *mut DBCompactionFilter {
        if let Some((db, _)) = need_filter_gc_table() {
            let name = CString::new("gc_compaction_filter").unwrap();
            let filter = Box::new(GcCompactionFilter::new(db));
            return unsafe { new_compaction_filter_raw(name, filter) };
        }
        std::ptr::null_mut()
    }
}

struct GcCompactionFilter {
    db: Arc<DB>,
    cf_def: &'static CFHandle,
}

impl GcCompactionFilter {
    fn new(db: Arc<DB>) -> Self {
        let db_ref: &'static DB = unsafe { std::mem::transmute(db.as_ref()) };
        let cf_def = rocks_util::get_cf_handle(db_ref, CF_DEFAULT).unwrap();
        GcCompactionFilter { db, cf_def }
    }
}

impl CompactionFilter for GcCompactionFilter {
    fn filter(
        &mut self,
        _level: usize,
        key: &[u8],
        _value: &[u8],
        _: &mut Vec<u8>,
        _: &mut bool,
    ) -> bool {
        if self.db.get_cf(self.cf_def, key).unwrap().is_none() {
            // The key in default cf has been cleared.
            return true;
        }
        false
    }
}

fn need_filter_gc_table() -> Option<(Arc<DB>, Arc<AtomicU64>)> {
    let gc_context_option = GC_CONTEXT.lock().unwrap();
    let gc_context = match *gc_context_option {
        Some(ref ctx) => ctx,
        None => return None,
    };

    let cf_handle = rocks_util::get_cf_handle(&gc_context.db, CF_GC).unwrap();
    let mut iter = gc_context.db.iter_cf(cf_handle);
    if iter.seek(SeekKey::Start).unwrap_or(false) {
        let db = Arc::clone(&gc_context.db);
        let safe_point = Arc::clone(&gc_context.safe_point);
        return Some((db, safe_point));
    }
    debug!("GC cf is empty, skip run compaction filter for default cf or gc cf");
    None
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::storage::kv::{RocksEngine, TestEngineBuilder};
    use crate::storage::mvcc::tests::{
        must_commit, must_get_none, must_prewrite_delete, must_prewrite_put,
    };
    use engine::rocks::util::{compact_range, get_cf_handle};
    use engine::rocks::Writable;
    use engine_traits::{CF_DEFAULT, CF_GC};

    pub fn gc_by_compact(engine: &RocksEngine, _: &[u8], safe_point: u64) {
        let kv = engine.get_rocksdb();
        // Put a new key-value pair to ensure compaction can be triggered correctly.
        let handle = get_cf_handle(&kv, "write").unwrap();
        kv.put_cf(handle, b"k1", b"v1").unwrap();

        let safe_point = Arc::new(AtomicU64::new(safe_point));
        let cfg = GcWorkerConfigManager(Arc::new(Default::default()));
        cfg.0.update(|v| v.enable_compaction_filter = true);
        init_compaction_filter(Arc::clone(&kv), safe_point, cfg);
        compact_range(&kv, handle, None, None, false, 1);
    }

    #[test]
    fn test_compaction_filter() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let db = engine.get_rocksdb();
        let cf_gc = get_cf_handle(&db, CF_GC).unwrap();
        let cf_def = get_cf_handle(&db, CF_DEFAULT).unwrap();
        let value = vec![b'x'; 256]; // long value.

        must_prewrite_put(&engine, b"key", &value, b"key", 5);
        must_commit(&engine, b"key", 5, 10);
        must_prewrite_put(&engine, b"key", &value, b"key", 15);
        must_commit(&engine, b"key", 15, 20);
        must_prewrite_delete(&engine, b"key", b"key", 25);

        // Run compaction filter on write cf can put `key+start_ts` into gc cf.
        gc_by_compact(&engine, b"", 22);
        must_get_none(&engine, b"key", 10);
        let k1 = Key::from_raw(b"key").append_ts(5.into()).into_encoded();
        assert!(engine.get_rocksdb().get_cf(cf_def, &k1).unwrap().is_some());
        assert!(engine.get_rocksdb().get_cf(cf_gc, &k1).unwrap().is_some());

        // Run compaction filter on gc cf won't delete a `key+start_ts` if it's still in default cf.
        db.put_cf(cf_gc, b"k1", b"v1").unwrap();
        compact_range(&db, cf_gc, None, None, false, 1);
        assert!(engine.get_rocksdb().get_cf(cf_gc, &k1).unwrap().is_some());

        // Run compaction filter on default cf.
        db.put_cf(cf_def, b"k1", b"v1").unwrap();
        compact_range(&db, cf_def, None, None, false, 1);
        assert!(engine.get_rocksdb().get_cf(cf_def, &k1).unwrap().is_none());

        // Run compaction filter on gc cf again.
        db.put_cf(cf_gc, b"k1", b"v1").unwrap();
        compact_range(&db, cf_gc, None, None, false, 1);
        assert!(engine.get_rocksdb().get_cf(cf_gc, &k1).unwrap().is_none());
    }
}
