// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::{util, RocksEngine};
use engine_traits::{Error, Range, Result};
use std::path::Path;

#[repr(transparent)]
pub struct UserCollectedProperties(rocksdb::UserCollectedProperties);
impl engine_traits::UserCollectedProperties for UserCollectedProperties {
    fn get(&self, index: &[u8]) -> Option<&[u8]> {
        self.0.get(index)
    }
}

#[repr(transparent)]
pub struct TablePropertiesCollection(rocksdb::TablePropertiesCollection);
impl engine_traits::TablePropertiesCollection for TablePropertiesCollection {
    type UserCollectedProperties = UserCollectedProperties;
    fn iter_user_collected_properties<F>(&self, mut f: F)
    where
        F: FnMut(&Self::UserCollectedProperties, &str) -> bool,
    {
        for (table, props) in (&self.0).into_iter() {
            let props = unsafe { std::mem::transmute(props.user_collected_properties()) };
            let table = Path::new(table)
                .file_name()
                .and_then(|x| x.to_str())
                .unwrap();
            if !f(props, table) {
                break;
            }
        }
    }
}

impl engine_traits::TablePropertiesExt for RocksEngine {
    type TablePropertiesCollection = TablePropertiesCollection;

    fn table_properties_collection(
        &self,
        cf: &str,
        ranges: &[Range],
    ) -> Result<Self::TablePropertiesCollection> {
        let collection = self.get_properties_of_tables_in_range(cf, ranges)?;
        Ok(TablePropertiesCollection(collection))
    }

    fn lsm_l0_tables(&self, cf: &str) -> Option<Vec<String>> {
        Some(lsm_l0_tables(self, cf))
    }
}

impl RocksEngine {
    pub(crate) fn get_properties_of_tables_in_range(
        &self,
        cf: &str,
        ranges: &[Range],
    ) -> Result<rocksdb::TablePropertiesCollection> {
        let cf = util::get_cf_handle(self.as_inner(), cf)?;
        // FIXME: extra allocation
        let ranges: Vec<_> = ranges.iter().map(util::range_to_rocks_range).collect();
        let raw = self
            .as_inner()
            .get_properties_of_tables_in_range(cf, &ranges);
        let raw = raw.map_err(Error::Engine)?;
        Ok(raw)
    }

    pub fn get_range_properties_cf(
        &self,
        cfname: &str,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<rocksdb::TablePropertiesCollection> {
        let range = Range::new(start_key, end_key);
        self.get_properties_of_tables_in_range(cfname, &[range])
    }
}

fn lsm_l0_tables(engine: &RocksEngine, cf: &str) -> Vec<String> {
    let cf_handle = crate::util::get_cf_handle(engine.as_inner(), cf).unwrap();
    let cf_metadata = engine.as_inner().get_column_family_meta_data(cf_handle);
    let mut res = Vec::with_capacity(8);
    for sst_metadata in cf_metadata.get_level(0).get_files() {
        let name = Path::new(&sst_metadata.get_name())
            .file_name()
            .and_then(|x| x.to_str())
            .unwrap()
            .to_owned();
        res.push(name);
    }
    res
}

#[cfg(test)]
mod tests {
    use super::*;
    use engine_traits::{MiscExt, SyncMutable, TablePropertiesCollection, TablePropertiesExt};
    use rocksdb::{ColumnFamilyOptions, DBOptions, DB};
    use std::sync::Arc;

    #[test]
    fn test_lsm_l0_tables() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let mut opts = DBOptions::default();
        opts.create_if_missing(true);
        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.set_disable_auto_compactions(true);
        let db = DB::open_cf(opts, path, vec![("default", cf_opts)]).unwrap();
        let engine = RocksEngine::from_db(Arc::new(db));
        for i in 0..4 {
            let k = format!("key-{}", i).into_bytes();
            engine.put(&k, b"value").unwrap();
            engine.flush(true).unwrap();
        }
        let tables = engine.lsm_l0_tables("default").unwrap();
        let mut count = 0;
        engine
            .table_properties_collection("default", &[Range::new(b"a", b"z")])
            .unwrap()
            .iter_user_collected_properties(|_, table| {
                count += 1;
                tables.iter().any(|x| x == table)
            });
        assert_eq!(count, 4);
    }

    // The benchmark shows getting L0 tables' names needs about 10k ns.
    #[bench]
    fn bench_lsm_l0_tables(b: &mut test::Bencher) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let mut opts = DBOptions::default();
        opts.create_if_missing(true);
        let db = DB::open(opts, path).unwrap();
        let engine = RocksEngine::from_db(Arc::new(db));
        for i in 0..4 {
            let k = format!("key-{}", i).into_bytes();
            engine.put(&k, b"value").unwrap();
            engine.flush(true).unwrap();
        }
        b.iter(|| assert!(engine.lsm_l0_tables("default").is_some()));
    }
}
