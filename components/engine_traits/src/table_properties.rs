// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::{Range, Result};

pub trait UserCollectedProperties {
    fn get(&self, index: &[u8]) -> Option<&[u8]>;
}

pub trait TablePropertiesCollection {
    type UserCollectedProperties: UserCollectedProperties;

    /// Iterator all `UserCollectedProperties`, break if `f` returns false.
    fn iter_user_collected_properties<F>(&self, f: F)
    where
        F: FnMut(&Self::UserCollectedProperties, &str /*table file name*/) -> bool;
}

pub trait TablePropertiesExt {
    type TablePropertiesCollection: TablePropertiesCollection;

    /// Collection of tables covering the given range.
    fn table_properties_collection(
        &self,
        cf: &str,
        ranges: &[Range],
    ) -> Result<Self::TablePropertiesCollection>;

    /// Level 0 tables' file names if the engine is LSM-like.
    fn lsm_l0_tables(&self, cf: &str) -> Option<Vec<String>>;
}
