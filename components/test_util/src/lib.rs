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

extern crate rand;
#[macro_use]
extern crate slog;
extern crate serde;
extern crate serde_json;
extern crate slog_scope;
extern crate time;
#[macro_use]
extern crate serde_derive;

extern crate tikv;

mod kv_generator;
mod logging;
mod security;

use std::env;
use std::fmt;

pub use kv_generator::*;
pub use logging::*;
pub use security::*;

pub const DEFAULT_ITERATIONS: usize = 1;
const DEFAULT_KEY_LENGTHS: [usize; 1] = [64];
const DEFAULT_VALUE_LENGTHS: [usize; 2] = [64, 65];

pub fn setup_for_ci() {
    let guard = if env::var("CI").is_ok() && env::var("LOG_FILE").is_ok() {
        Some(logging::init_log())
    } else {
        None
    };
    if env::var("PANIC_ABORT").is_ok() {
        // Panics as aborts, it's helpful for debugging,
        // but also stops tests immediately.
        tikv::util::set_exit_hook(true, guard, "./");
    } else if let Some(guard) = guard {
        // Do not reset the global logger.
        guard.cancel_reset();
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct KvConfig {
    pub key_length: usize,
    pub value_length: usize,
}

impl fmt::Debug for KvConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let s = serde_json::to_string(self).unwrap();
        write!(f, "{}", s.replace("\"", "+"))
    }
}

pub fn generate_kv_configs() -> Vec<KvConfig> {
    //    let key_lengths = vec![64, 128];
    let key_lengths = DEFAULT_KEY_LENGTHS;
    //    let value_lengths = vec![64, 65, 128, 1024, 1024 * 16];
    let value_lengths = DEFAULT_VALUE_LENGTHS;
    let mut configs = vec![];

    for &kl in &key_lengths {
        for &vl in &value_lengths {
            configs.push(KvConfig {
                key_length: kl,
                value_length: vl,
            })
        }
    }
    configs
}

#[derive(Copy, Clone)]
pub enum Level {
    Storage,
    Txn,
    Mvcc,
    Engine,
}

impl fmt::Display for Level {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Level::Storage => write!(f, "storage"),
            Level::Txn => write!(f, "txn"),
            Level::Mvcc => write!(f, "mvcc"),
            Level::Engine => write!(f, "engine"),
        }
    }
}

#[allow(dead_code)]
pub fn get_full_method_name(level: Level, name: &str) -> String {
    format!("{}::{}", level, name)
}
