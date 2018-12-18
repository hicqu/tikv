// Copyright 2016 PingCAP, Inc.
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
use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use kvproto::import_sstpb::SSTMeta;
use kvproto::metapb;
use kvproto::metapb::RegionEpoch;
use kvproto::pdpb::CheckPolicy;
use kvproto::raft_cmdpb::{RaftCmdRequest, RaftCmdResponse};
use kvproto::raft_serverpb::{RaftMessage, RegionLocalState};

use raft::{SnapshotStatus, StateRole};
use raftstore::store::util::KeysInfoFormatter;
use raftstore::store::Config;
use util::escape;
use util::rocksdb::CompactedEvent;

use super::{RegionSnapshot, SnapKey};
use raftstore::store::fsm::apply::TaskRes as ApplyTaskRes;

#[derive(Debug, Clone)]
pub struct ReadResponse {
    pub response: RaftCmdResponse,
    pub snapshot: Option<RegionSnapshot>,
}

#[derive(Debug)]
pub struct WriteResponse {
    pub response: RaftCmdResponse,
}

#[derive(Debug)]
pub enum SeekRegionResult {
    Found(metapb::Region),
    LimitExceeded { next_key: Vec<u8> },
    Ended,
}

pub type ReadCallback = Box<FnBox(ReadResponse) + Send>;
pub type WriteCallback = Box<FnBox(WriteResponse) + Send>;

pub type SeekRegionCallback = Box<FnBox(SeekRegionResult) + Send>;
pub type SeekRegionFilter = Box<Fn(&metapb::Region, StateRole) -> bool + Send>;

/// Variants of callbacks for `Msg`.
///  - `Read`: a callbak for read only requests including `StatusRequest`,
///         `GetRequest` and `SnapRequest`
///  - `Write`: a callback for write only requests including `AdminRequest`
///          `PutRequest`, `DeleteRequest` and `DeleteRangeRequest`.
pub enum Callback {
    /// No callback.
    None,
    /// Read callback.
    Read(ReadCallback),
    /// Write callback.
    Write(WriteCallback),
}

impl Callback {
    pub fn invoke_with_response(self, resp: RaftCmdResponse) {
        match self {
            Callback::None => (),
            Callback::Read(read) => {
                let resp = ReadResponse {
                    response: resp,
                    snapshot: None,
                };
                read(resp);
            }
            Callback::Write(write) => {
                let resp = WriteResponse { response: resp };
                write(resp);
            }
        }
    }

    pub fn invoke_read(self, args: ReadResponse) {
        match self {
            Callback::Read(read) => read(args),
            Callback::None => (),
            other => panic!("expect Callback::Read(..), got {:?}", other),
        }
    }
}

impl fmt::Debug for Callback {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Callback::None => write!(fmt, "Callback::None"),
            Callback::Read(_) => write!(fmt, "Callback::Read(..)"),
            Callback::Write(_) => write!(fmt, "Callback::Write(..)"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum PeerTick {
    Raft,
    RaftLogGc,
    SplitRegionCheck,
    PdHeartbeat,
    CheckMerge,
    CheckPeerStaleState,
}

#[derive(Debug, Clone, Copy)]
pub enum StoreTick {
    CompactCheck,
    CompactLockCf,
    PdStoreHeartbeat,
    SnapGc,
    CleanupImportSST,
    ConsistencyCheck,
}

impl StoreTick {
    #[inline]
    pub fn tag(self) -> &'static str {
        match self {
            StoreTick::CompactCheck => "compact_check",
            StoreTick::PdStoreHeartbeat => "pd_store_heartbeat",
            StoreTick::SnapGc => "snap_gc",
            StoreTick::CompactLockCf => "compact_lock_cf",
            StoreTick::ConsistencyCheck => "consistency_check",
            StoreTick::CleanupImportSST => "cleanup_import_sst",
        }
    }
}

impl PeerTick {
    #[inline]
    pub fn tag(self) -> &'static str {
        match self {
            PeerTick::Raft => "raft",
            PeerTick::RaftLogGc => "raft_log_gc",
            PeerTick::SplitRegionCheck => "split_region_check",
            PeerTick::PdHeartbeat => "pd_heartbeat",
            PeerTick::CheckMerge => "check_merge",
            PeerTick::CheckPeerStaleState => "check_peer_stale_state",
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum SignificantMsg {
    SnapshotStatus {
        to_peer_id: u64,
        status: SnapshotStatus,
    },
    Unreachable {
        to_peer_id: u64,
    },
}

pub enum PeerMsg {
    // For notify.
    RaftMessage(RaftMessage),

    RaftCmd {
        send_time: Instant,
        request: RaftCmdRequest,
        callback: Callback,
    },

    SplitRegion {
        region_epoch: RegionEpoch,
        // It's an encoded key.
        // TODO: support meta key.
        split_keys: Vec<Vec<u8>>,
        callback: Callback,
    },

    // For consistency check
    ComputeHashResult {
        index: u64,
        hash: Vec<u8>,
    },

    // For region size
    RegionApproximateSize {
        size: u64,
    },

    // For region keys
    RegionApproximateKeys {
        keys: u64,
    },

    CompactionDeclinedBytes(u64),

    HalfSplitRegion {
        region_epoch: RegionEpoch,
        policy: CheckPolicy,
    },

    MergeResult {
        target: metapb::Peer,
        stale: bool,
    },
    GcSnap(Vec<(SnapKey, bool)>),

    Tick(PeerTick),
    SignificantMsg(SignificantMsg),
    ApplyRes(ApplyTaskRes),
    Start {
        state: Option<RegionLocalState>,
    },
    ClearStat,
    Noop,
}

pub enum StoreMsg {
    Start(metapb::Store, Arc<Config>),

    // Redirect to store if region not found.
    RaftMessage(RaftMessage),

    // For snapshot stats.
    SnapshotStats,

    // Compaction finished event
    CompactedEvent(CompactedEvent),

    ValidateSSTResult {
        invalid_ssts: Vec<SSTMeta>,
    },

    SeekRegion {
        from_key: Vec<u8>,
        filter: SeekRegionFilter,
        limit: u32,
        callback: SeekRegionCallback,
    },

    Tick(StoreTick),
    // Clear region size and keys for all regions in the range, so we can force them to re-calculate
    // their size later.
    ClearRegionSizeInRange {
        start_key: Vec<u8>,
        end_key: Vec<u8>,
    },
}

impl fmt::Debug for PeerMsg {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            PeerMsg::RaftMessage(_) => write!(fmt, "Raft Message"),
            PeerMsg::RaftCmd { .. } => write!(fmt, "Raft Command"),
            PeerMsg::ComputeHashResult { index, ref hash } => write!(
                fmt,
                "ComputeHashResult [index: {}, hash: {}]",
                index,
                escape(hash)
            ),
            PeerMsg::SplitRegion { ref split_keys, .. } => {
                write!(fmt, "Split at key {}", KeysInfoFormatter(split_keys))
            }
            PeerMsg::RegionApproximateSize { size } => {
                write!(fmt, "Region's approximate size [size: {:?}]", size)
            }
            PeerMsg::RegionApproximateKeys { keys } => {
                write!(fmt, "Region's approximate keys [keys: {:?}]", keys)
            }
            PeerMsg::CompactionDeclinedBytes(bytes) => {
                write!(fmt, "Compaction declined bytes {}", bytes)
            }
            PeerMsg::HalfSplitRegion { .. } => write!(fmt, "Half Split region"),
            PeerMsg::MergeResult { ref target, stale } => write!(
                fmt,
                "MergeResult source: {:?}, successful: {}",
                target, stale
            ),
            PeerMsg::GcSnap(_) => write!(fmt, "GcSnap"),
            PeerMsg::Tick(t) => write!(fmt, "{:?}", t),
            PeerMsg::SignificantMsg(ref msg) => write!(fmt, "{:?}", msg),
            PeerMsg::ApplyRes(_) => write!(fmt, "ApplyRes"),
            PeerMsg::Start { ref state } => write!(fmt, "Start {:?}", state),
            PeerMsg::ClearStat => write!(fmt, "ClearStat"),
            PeerMsg::Noop => write!(fmt, "Noop"),
        }
    }
}

impl PeerMsg {
    pub fn new_raft_cmd(request: RaftCmdRequest, callback: Callback) -> PeerMsg {
        PeerMsg::RaftCmd {
            send_time: Instant::now(),
            request,
            callback,
        }
    }

    pub fn new_half_split_region(region_epoch: RegionEpoch, policy: CheckPolicy) -> PeerMsg {
        PeerMsg::HalfSplitRegion {
            region_epoch,
            policy,
        }
    }
}

impl fmt::Debug for StoreMsg {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            StoreMsg::RaftMessage(_) => write!(fmt, "Raft Message"),
            StoreMsg::SnapshotStats => write!(fmt, "Snapshot stats"),
            StoreMsg::CompactedEvent(ref event) => write!(fmt, "CompactedEvent cf {}", event.cf),
            StoreMsg::ValidateSSTResult { .. } => write!(fmt, "Validate SST Result"),
            StoreMsg::SeekRegion { ref from_key, .. } => {
                write!(fmt, "Seek Region from_key {:?}", from_key)
            }
            StoreMsg::ClearRegionSizeInRange {
                ref start_key,
                ref end_key,
            } => write!(
                fmt,
                "Clear Region size in range {:?} to {:?}",
                start_key, end_key
            ),
            StoreMsg::Tick(t) => write!(fmt, "{:?}", t),
            StoreMsg::Start(ref meta, _) => write!(fmt, "Store {}", meta.get_id()),
        }
    }
}
