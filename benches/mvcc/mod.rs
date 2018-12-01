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
extern crate test_util;
extern crate tikv;

use criterion::{black_box, Bencher, Criterion};
use kvproto::kvrpcpb::Context;
use test_util::*;
use tikv::storage::engine::{BTreeEngine, Engine, Modify, Snapshot, TestEngineBuilder};
use tikv::storage::mvcc::{MvccReader, MvccTxn};
use tikv::storage::{Key, Mutation, Options, CF_LOCK, CF_WRITE};

fn make_engine() -> impl Engine {
    TestEngineBuilder::new().build().unwrap()
    //        BTreeEngine::default()
}

fn mvcc_prewrite(b: &mut Bencher, config: &KvConfig) {
    let engine = make_engine();
    let ctx = Context::new();
    let option = Options::default();
    b.iter_with_setup(
        || {
            let mutations: Vec<(Mutation, Vec<u8>)> =
                generate_deliberate_kvs(DEFAULT_ITERATIONS, config.key_length, config.value_length)
                    .iter()
                    .map(|(k, v)| (Mutation::Put((Key::from_raw(&k), v.clone())), k.clone()))
                    .collect();
            (mutations, &option)
        },
        |(mutations, option)| {
            for (mutation, primary) in mutations {
                let snapshot = engine.snapshot(&ctx).unwrap();
                let mut txn = MvccTxn::new(snapshot.clone(), 1, false).unwrap();
                txn.prewrite(mutation, &primary, option);
                let modifies = txn.into_modifies();
                let _ = engine.write(&ctx, modifies);
            }
        },
    )
}

fn mvcc_commit(b: &mut Bencher, config: &KvConfig) {
    let engine = make_engine();
    let ctx = Context::new();
    let snapshot = engine.snapshot(&ctx).unwrap();
    let option = Options::default();
    b.iter_with_setup(
        || {
            let mut txn = MvccTxn::new(snapshot.clone(), 1, false).unwrap();

            let kvs =
                generate_deliberate_kvs(DEFAULT_ITERATIONS, config.key_length, config.value_length);
            for (k, v) in &kvs {
                txn.prewrite(
                    Mutation::Put((Key::from_raw(&k), v.clone())),
                    &k.clone(),
                    &option,
                );
            }
            let modifies = txn.into_modifies();
            let _ = engine.write(&ctx, modifies);
            let keys: Vec<Key> = kvs.iter().map(|(k, v)| Key::from_raw(&k)).collect();
            keys
        },
        |keys| {
            let snapshot = engine.snapshot(&ctx).unwrap();
            for key in keys {
                let mut txn = MvccTxn::new(snapshot.clone(), 1, false).unwrap();
                txn.commit(key, 1);
                let modifies = txn.into_modifies();
                let _ = engine.write(&ctx, modifies);
            }
        },
    );
}

fn mvcc_reader_load_lock(b: &mut Bencher, config: &KvConfig) {
    let engine = make_engine();
    let ctx = Context::default();
    let test_keys: Vec<Key> =
        generate_random_kvs(DEFAULT_ITERATIONS, config.key_length, config.value_length)
            .iter()
            .map(|(k, _)| Key::from_raw(&k))
            .collect();

    b.iter_with_setup(
        || {
            let snapshot = engine.snapshot(&ctx).unwrap();
            let reader = MvccReader::new(snapshot, None, false, None, None, ctx.isolation_level);
            (reader, &test_keys)
        },
        |(mut reader, test_kvs)| {
            for key in test_kvs {
                black_box(reader.load_lock(&key).is_ok());
            }
        },
    );
}

fn mvcc_reader_seek_write(b: &mut Bencher, config: &KvConfig) {
    let engine = make_engine();
    let ctx = Context::default();
    b.iter_with_setup(
        || {
            let snapshot = engine.snapshot(&ctx).unwrap();
            let reader = MvccReader::new(snapshot, None, false, None, None, ctx.isolation_level);
            let test_keys: Vec<Key> =
                generate_random_kvs(DEFAULT_ITERATIONS, config.key_length, config.value_length)
                    .iter()
                    .map(|(k, _)| Key::from_raw(&k))
                    .collect();
            (reader, test_keys)
        },
        |(mut reader, test_keys)| {
            for key in &test_keys {
                black_box(reader.seek_write(&key, u64::max_value()).is_ok());
            }
        },
    );
}

fn mvcc_bench(c: &mut Criterion) {
    c.bench_function_over_inputs(
        &get_full_method_name(Level::Mvcc, "prewrite"),
        mvcc_prewrite,
        generate_kv_configs(),
    );
    c.bench_function_over_inputs(
        &get_full_method_name(Level::Mvcc, "commit"),
        mvcc_commit,
        generate_kv_configs(),
    );
    c.bench_function_over_inputs(
        &get_full_method_name(Level::Mvcc, "load_lock"),
        mvcc_reader_load_lock,
        generate_kv_configs(),
    );
    c.bench_function_over_inputs(
        &get_full_method_name(Level::Mvcc, "seek_write"),
        mvcc_reader_seek_write,
        generate_kv_configs(),
    );
}

criterion_group!(benches, mvcc_bench);
criterion_main!(benches);
