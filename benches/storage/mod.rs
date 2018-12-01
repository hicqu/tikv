// Copyright 2018 PingCAP, Inc.
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

#[macro_use]
extern crate criterion;
extern crate kvproto;
extern crate test_storage;
extern crate test_util;
extern crate tikv;

use criterion::{black_box, Bencher, Criterion};
use kvproto::kvrpcpb::Context;
use test_storage::SyncTestStorageBuilder;
use test_util::*;
use tikv::storage::{Key, Mutation};

fn storage_prewrite(b: &mut Bencher, config: &KvConfig) {
    let store = SyncTestStorageBuilder::new().build().unwrap();
    b.iter_with_setup(
        || {
            let kvs =
                generate_deliberate_kvs(DEFAULT_ITERATIONS, config.key_length, config.value_length);

            let data: Vec<(Context, Vec<Mutation>, Vec<u8>)> = kvs.iter().map(|(k, v)|
                (Context::new(), vec![Mutation::Put((Key::from_raw(&k), v.clone()))], k.clone())).collect();
            (data, &store)
        },
        |(data, store)| {
            for (context, mutations, primary) in data {
                store
                    .prewrite(
                        context,
                        mutations,
                        primary,
                        1,
                    ).unwrap();
            }
        },
    );
}

fn storage_commit(b: &mut Bencher, config: &KvConfig) {
    let store = SyncTestStorageBuilder::new().build().unwrap();

    b.iter_with_setup(
        || {
            let kvs =
                generate_random_kvs(DEFAULT_ITERATIONS, config.key_length, config.value_length);

            for (k, v) in &kvs {
                store
                    .prewrite(
                        Context::new(),
                        vec![Mutation::Put((Key::from_raw(&k), v.clone()))],
                        k.clone(),
                        1,
                    )
                    .expect("");
            }

            (kvs, &store)
        },
        |(kvs, store)| {
            for (k, v) in &kvs {
                store
                    .commit(Context::new(), vec![Key::from_raw(k)], 1, 2)
                    .expect("");
            }
        },
    );
}

fn bench_storage(c: &mut Criterion) {
    c.bench_function_over_inputs(
        &get_full_method_name(Level::Storage, "async_prewrite"),
        storage_prewrite,
        generate_kv_configs(),
    );
    c.bench_function_over_inputs(
        &get_full_method_name(Level::Storage, "async_commit"),
        storage_commit,
        generate_kv_configs(),
    );
}

criterion_group!(benches, bench_storage,);
criterion_main!(benches);
