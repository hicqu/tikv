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

use criterion::{black_box, Criterion};
use kvproto::kvrpcpb::Context;
use test_util::{generate_deliberate_kvs, generate_random_kvs};
use tikv::storage::engine::{BTreeEngine, Engine, Modify, Snapshot, TestEngineBuilder};
use tikv::storage::mvcc::{MvccReader, MvccTxn};
use tikv::storage::{Key, Mutation, Options, CF_LOCK, CF_WRITE};

const DEFAULT_ITERATIONS: usize = 1;
const DEFAULT_KEY_LENGTH: usize = 64;
const DEFAULT_VALUE_LENGTH: usize = 65;

fn make_engine() -> impl Engine {
    TestEngineBuilder::new().build().unwrap()
//        BTreeEngine::default()
}

fn fill_engine_with<E: Engine>(
    engine: &E,
    expect_engine_keys_count: usize,
    value_length: usize,
    cf: &'static str,
) {
    let mut modifies: Vec<Modify> = vec![];
    if expect_engine_keys_count > 0 {
        let kvs = generate_random_kvs(expect_engine_keys_count, DEFAULT_KEY_LENGTH, value_length);
        for (key, value) in kvs {
            modifies.push(Modify::Put(cf, Key::from_raw(&key), value))
        }
    }
    let ctx = Context::new();
    let _ = engine.async_write(&ctx, modifies, Box::new(move |(_, _)| {}));
}

fn mvcc_txn_prewrite_value_is_none(c: &mut Criterion) {
    c.bench_function("prewrite_value_is_none", |b| {
        let engine = make_engine();
        let ctx = Context::new();
        let option = Options::default();

        b.iter_with_setup(
            || {
                let snapshot = engine.snapshot(&ctx).unwrap();
                let mutations: Vec<(Mutation, Vec<u8>)> = generate_deliberate_kvs(
                    DEFAULT_ITERATIONS,
                    DEFAULT_KEY_LENGTH,
                    DEFAULT_VALUE_LENGTH,
                ).iter()
                    .map(|(k, _)| (Mutation::Delete(Key::from_raw(&k)), k.clone()))
                    .collect();

                let txn = MvccTxn::new(snapshot.clone(), 1, false).unwrap();
                (mutations, txn, &option)
            },
            |(mutations, mut txn, option)| {
                for (mutation, primary) in mutations {
                    black_box(txn.prewrite(mutation, &primary, option).unwrap());
                }
            },
        )
    });
}

fn mvcc_txn_prewrite_value_is_short(c: &mut Criterion) {
    c.bench_function("prewrite_value_is_short", |b| {
        let engine = make_engine();
        let ctx = Context::new();
        let option = Options::default();
        b.iter_with_setup(
            || {
                let snapshot = engine.snapshot(&ctx).unwrap();
                let mutations: Vec<(Mutation, Vec<u8>)> =
                    generate_deliberate_kvs(DEFAULT_ITERATIONS, DEFAULT_KEY_LENGTH, 64)
                        .iter()
                        .map(|(k, v)| (Mutation::Put((Key::from_raw(&k), v.clone())), k.clone()))
                        .collect();
                let txn = MvccTxn::new(snapshot.clone(), 1, false).unwrap();
                (mutations, txn, &option)
            },
            |(mutations, mut txn, option)| {
                for (mutation, primary) in mutations {
                    black_box(txn.prewrite(mutation, &primary, option).unwrap());
                }
            },
        )
    });
}

fn mvcc_txn_prewrite_value_is_long(c: &mut Criterion) {
    c.bench_function("prewrite_value_is_long", |b| {
        let engine = make_engine();
        let ctx = Context::new();
        let option = Options::default();
        b.iter_with_setup(
            || {
                let snapshot = engine.snapshot(&ctx).unwrap();
                let mutations: Vec<(Mutation, Vec<u8>)> =
                    generate_deliberate_kvs(DEFAULT_ITERATIONS, DEFAULT_KEY_LENGTH, 65)
                        .iter()
                        .map(|(k, v)| (Mutation::Put((Key::from_raw(&k), v.clone())), k.clone()))
                        .collect();
                let txn = MvccTxn::new(snapshot.clone(), 1, false).unwrap();
                (mutations, txn, &option)
            },
            |(mutations, mut txn, option)| {
                for (mutation, primary) in mutations {
                    black_box(txn.prewrite(mutation, &primary, option).unwrap());
                }
            },
        )
    });
}

fn commit<E, I>(engine: &E, kvs: &[(Vec<u8>, Vec<u8>)], ts_generator: &mut I, write_down: bool)
where
    E: Engine,
    I: Iterator<Item = u64>,
{
    let ctx = Context::new();
    let snapshot = engine.snapshot(&ctx).unwrap();
    for (k, _) in kvs {
        let start_ts = ts_generator.next().unwrap();
        let mut txn = MvccTxn::new(snapshot.clone(), start_ts, false).unwrap();
        txn.commit(Key::from_raw(k.as_slice()), start_ts).unwrap();
        if write_down {
            let modifies = txn.into_modifies();
            let _ = engine.async_write(&ctx, modifies, Box::new(|(_, _)| {}));
        }
    }
}
//fn mvcc_reader_scan_lock(
//    bencher: &mut Bencher,
//    iterations: usize,
//    limit: usize,
//    lock_cf_size: usize,
//) {
//    let engine = make_engine();
//    let ctx = Context::default();
//    let snapshot = engine.snapshot(&ctx).unwrap();
//   fill_engine_with(&engine,1000,DEFAULT_VALUE_LENGTH);
//
//    let mut reader = MvccReader::new(snapshot, None, false, None, None, ctx.isolation_level);
//    let test_kvs = generate_random_kvs(DEFAULT_ITERATIONS, DEFAULT_KEY_LENGTH, DEFAULT_VALUE_LENGTH);
//    bencher.iter(|| {
//        for (k, _) in &test_kvs {
//            reader
//                .scan_locks(
//                    Some(&Key::from_raw(k.as_slice())),
//                    |lock| lock.ts < lock_cf_size as u64,
//                    limit,
//                )
//                .is_ok();
//        }
//    });
//}

fn mvcc_reader_load_lock(c: &mut Criterion) {
    c.bench_function("reader_load_lock", |bencher| {
        let engine = make_engine();
        let ctx = Context::default();
        let test_keys: Vec<Key> =
            generate_random_kvs(DEFAULT_ITERATIONS, DEFAULT_KEY_LENGTH, DEFAULT_VALUE_LENGTH)
                .iter()
                .map(|(k, _)| Key::from_raw(&k))
                .collect();

        bencher.iter_with_setup(
            || {
                let snapshot = engine.snapshot(&ctx).unwrap();
                let reader =
                    MvccReader::new(snapshot, None, false, None, None, ctx.isolation_level);
                (reader, &test_keys)
            },
            |(mut reader, test_kvs)| {
                for key in test_kvs {
                    black_box(reader.load_lock(&key).is_ok());
                }
            },
        );
    });
}

fn mvcc_reader_seek_write(c: &mut Criterion) {
    c.bench_function("reader_seek_write", |bencher| {
        let engine = make_engine();
        let ctx = Context::default();

        bencher.iter_with_setup(
            || {
                let snapshot = engine.snapshot(&ctx).unwrap();
                let reader =
                    MvccReader::new(snapshot, None, false, None, None, ctx.isolation_level);
                let test_keys: Vec<Key> = generate_random_kvs(
                    DEFAULT_ITERATIONS,
                    DEFAULT_KEY_LENGTH,
                    DEFAULT_VALUE_LENGTH,
                ).iter()
                    .map(|(k, _)| Key::from_raw(&k))
                    .collect();
                (reader, test_keys)
            },
            |(mut reader, test_keys)| {
                for key in &test_keys {
                    black_box(reader.seek_write(&key, u64::max_value()).is_ok());
                }
            },
        )
    });
}

fn bench_btree_clone(c: &mut Criterion) {
    use std::collections::BTreeMap;
    c.bench_function("clone", |b| {
        b.iter_with_setup(
            || {
                let mut tree = BTreeMap::new();
                let kvs = generate_random_kvs(1000, DEFAULT_KEY_LENGTH, DEFAULT_VALUE_LENGTH);
                for (key, value) in kvs {
                    tree.insert(Key::from_raw(&key), value);
                }
                tree
            },
            |tree| black_box(tree.clone()),
        )
    });
}

criterion_group!(
    benches,
    mvcc_txn_prewrite_value_is_none,
    mvcc_txn_prewrite_value_is_short,
    mvcc_txn_prewrite_value_is_long,
    mvcc_reader_seek_write,
    mvcc_reader_load_lock,
);
criterion_main!(benches);

//fn main() {
//    let mut criterion = Criterion::default().sample_size(20);
////    mvcc_txn_prewrite_value_is_none(&mut criterion);
//    mvcc_txn_prewrite_value_is_short(&mut criterion);
////    mvcc_txn_prewrite_value_is_long(&mut criterion);
////    mvcc_reader_seek_write(&mut criterion);
////    mvcc_reader_load_lock(&mut criterion);
//    criterion.final_summary();
//}

//



#[test]
fn load_lock_statistics() {
    let engine = make_engine();
    //        let engine = TestEngineBuilder::new().build().unwrap();
    let ctx = Context::default();
    fill_engine_with(&engine, 1000, DEFAULT_VALUE_LENGTH, CF_LOCK);

    let snapshot = engine.snapshot(&ctx).unwrap();
    let mut reader = MvccReader::new(snapshot, None, false, None, None, ctx.isolation_level);
    let test_keys: Vec<Key> = generate_random_kvs(1, DEFAULT_KEY_LENGTH, DEFAULT_VALUE_LENGTH)
        .iter()
        .map(|(k, _)| Key::from_raw(&k))
        .collect();

    for key in &test_keys {
        black_box(reader.load_lock(&key).is_ok());
    }
    println!("finished.{:?}", reader.get_statistics().details())
}

#[test]
fn prewrite_statistics() {
    let engine = make_engine();
    let ctx = Context::new();
    let snapshot = engine.snapshot(&ctx).unwrap();
    let option = Options::default();
    let kvs: Vec<(Key, Vec<u8>, Vec<u8>)> =
        generate_random_kvs(DEFAULT_ITERATIONS, DEFAULT_KEY_LENGTH, DEFAULT_VALUE_LENGTH)
            .iter()
            .map(|(k, v)| (Key::from_raw(&k), k.clone(), v.clone()))
            .collect();
    let mut txn = MvccTxn::new(snapshot.clone(), 1, false).unwrap();

    for (k, primary, v) in kvs {
        txn.prewrite(Mutation::Put((k, v)), primary.as_ref(), &option)
            .is_ok();
    }
    println!(
        "option:skip_constraint_check {:?}",
        option.skip_constraint_check
    );
    println!("prewrite_statistics.{:?}", txn.take_statistics().details())
}
