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

use std::fmt::{self, Display, Formatter};
use std::boxed::FnBox;
use std::time::{Duration, Instant};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::{future, Async, Future, Poll, Stream};
use futures_cpupool::{Builder as CpuPoolBuilder, CpuPool};
use grpc::{ChannelBuilder, ClientStreamingSink, Environment, RequestStream, RpcStatus,
           RpcStatusCode, WriteFlags};
use kvproto::raft_serverpb::{Done, SnapshotChunk};
use kvproto::raft_serverpb::RaftMessage;
use kvproto::tikvpb_grpc::TikvClient;

use raftstore::store::{SnapEntry, SnapKey, SnapManager, Snapshot};
use util::DeferContext;
use util::worker::Runnable;
use util::security::SecurityManager;

use super::metrics::*;
use super::{Error, Result};
use super::transport::RaftStoreRouter;

pub type Callback = Box<FnBox(Result<()>) + Send>;

const DEFAULT_POOL_SIZE: usize = 3;
// How many snapshots can be sent concurrently.
const MAX_SENDER_CONCURRENT: usize = 16;
// How many snapshots can be recv concurrently.
const MAX_RECEIVER_CONCURRENT: usize = 16;

pub enum Task {
    Recv {
        stream: RequestStream<SnapshotChunk>,
        sink: ClientStreamingSink<Done>,
    },
    SendTo {
        addr: String,
        msg: RaftMessage,
        cb: Callback,
    },
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Task::Recv { .. } => write!(f, "Recv"),
            Task::SendTo {
                ref addr, ref msg, ..
            } => write!(f, "SendTo Snap[to: {}, snap: {:?}]", addr, msg),
        }
    }
}

struct SnapChunk {
    first: Option<SnapshotChunk>,
    snap: Box<Snapshot>,
    remain_bytes: usize,
}

const SNAP_CHUNK_LEN: usize = 1024 * 1024;

impl Stream for SnapChunk {
    type Item = (SnapshotChunk, WriteFlags);
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Error> {
        if let Some(t) = self.first.take() {
            let write_flags = WriteFlags::default().buffer_hint(true);
            return Ok(Async::Ready(Some((t, write_flags))));
        }

        let mut buf = match self.remain_bytes {
            0 => return Ok(Async::Ready(None)),
            n if n > SNAP_CHUNK_LEN => vec![0; SNAP_CHUNK_LEN],
            n => vec![0; n],
        };
        let result = self.snap.read_exact(buf.as_mut_slice());
        match result {
            Ok(_) => {
                self.remain_bytes -= buf.len();
                let mut chunk = SnapshotChunk::new();
                chunk.set_data(buf);
                Ok(Async::Ready(Some((
                    chunk,
                    WriteFlags::default().buffer_hint(true),
                ))))
            }
            Err(e) => Err(box_err!("failed to read snapshot chunk: {}", e)),
        }
    }
}

struct SendStat {
    key: SnapKey,
    total_size: u64,
    elapsed: Duration,
}

/// Send the snapshot to specified address.
///
/// It will first send the normal raft snapshot message and then send the snapshot file.
fn send_snap(
    env: Arc<Environment>,
    mgr: SnapManager,
    security_mgr: Arc<SecurityManager>,
    addr: &str,
    msg: RaftMessage,
) -> Result<impl Future<Item = SendStat, Error = Error>> {
    assert!(msg.get_message().has_snapshot());
    let timer = Instant::now();

    let send_timer = SEND_SNAP_HISTOGRAM.start_coarse_timer();

    let key = {
        let snap = msg.get_message().get_snapshot();
        SnapKey::from_snap(snap)?
    };

    mgr.register(key.clone(), SnapEntry::Sending);
    let deregister = {
        let (mgr, key) = (mgr.clone(), key.clone());
        DeferContext::new(move || mgr.deregister(&key, &SnapEntry::Sending))
    };

    let s = box_try!(mgr.get_snapshot_for_sending(&key));
    if !s.exists() {
        return Err(box_err!("missing snap file: {:?}", s.path()));
    }
    let total_size = s.total_size()?;

    let chunks = {
        let mut first_chunk = SnapshotChunk::new();
        first_chunk.set_message(msg);

        SnapChunk {
            first: Some(first_chunk),
            snap: s,
            remain_bytes: total_size as usize,
        }
    };

    let cb = ChannelBuilder::new(env);
    let channel = security_mgr.connect(cb, addr);
    let client = TikvClient::new(channel);
    let (sink, receiver) = client.snapshot()?;

    let send = chunks.forward(sink).map_err(Error::from);
    let send = send.and_then(|(s, _)| receiver.map_err(Error::from).map(|_| s))
        .then(move |result| {
            send_timer.observe_duration();
            drop(deregister);
            drop(client);
            result.map(|s| {
                s.snap.delete();
                // TODO: improve it after rustc resolves the bug.
                // Call `info` in the closure directly will cause rustc
                // panic with `Cannot create local mono-item for DefId`.
                SendStat {
                    key: key,
                    total_size: total_size,
                    elapsed: timer.elapsed(),
                }
            })
        });
    Ok(send)
}

struct RecvSnapContext {
    key: SnapKey,
    file: Option<Box<Snapshot>>,
    raft_msg: RaftMessage,
}

fn recv_snap<R: RaftStoreRouter + 'static>(
    stream: RequestStream<SnapshotChunk>,
    sink: ClientStreamingSink<Done>,
    snap_mgr: SnapManager,
    raft_router: R,
) -> impl Future<Item = (), Error = ()> {
    let finish_context = move |context: RecvSnapContext| {
        let key = context.key;
        if let Some(mut file) = context.file {
            if let Err(e) = file.save() {
                let path = file.path();
                error!("{} failed to save snapshot file {}: {:?}", key, path, e);
                return Err(());
            }
        }
        raft_router.send_raft_msg(context.raft_msg).map_err(|e| {
            error!("{} failed to send snapshot to raft: {}", key, e);
        })?;
        Ok(())
    };

    let stream = stream.map_err(|e| error!("receive snapshot chunks from gRPC fail: {}", e));

    let f = stream.into_future().map_err(|_| ()).and_then(
        move |(head, chunks)| -> Box<Future<Item = (), Error = ()> + Send> {
            // Whether the stream is empty or the snapshot is corrupted,
            // we can let sender delete it simply by return Ok here.
            let context = match get_context_from_head_chunk(head, &snap_mgr) {
                Ok(context) => context,
                Err(_) => return box future::ok(()),
            };

            if context.file.is_none() {
                return box future::result(finish_context(context));
            }

            let context_key = context.key.clone();
            snap_mgr.register(context.key.clone(), SnapEntry::Receiving);

            let chunks = chunks.map_err(|_| false);
            let recv_chunks = chunks.fold(context, move |mut context, mut chunk| {
                let data = chunk.take_data();
                if !data.is_empty() {
                    let (key, file) = (&context.key, context.file.as_mut().unwrap());
                    if let Err(e) = file.write_all(&data) {
                        let path = file.path();
                        error!("{} failed to write snapshot file {}: {}", key, path, e);
                        return Err(true);
                    }
                } else {
                    error!("{} receive chunk with empty data", context.key);
                    return Err(true);
                }
                Ok(context)
            });

            box recv_chunks.then(move |result| {
                defer!(snap_mgr.deregister(&context_key, &SnapEntry::Receiving));
                match result {
                    Ok(context) => finish_context(context),
                    Err(true) => Ok(()), // let sender delete the snapshot simply.
                    Err(false) => Err(()),
                }
            })
        },
    );
    f.and_then(move |_| sink.success(Done::new()).map_err(|_| ()))
}

fn get_context_from_head_chunk(
    head_chunk: Option<SnapshotChunk>,
    snap_mgr: &SnapManager,
) -> ::std::result::Result<RecvSnapContext, ()> {
    // head_chunk is None means the stream is empty.
    let mut head = head_chunk.ok_or(())?;
    if !head.has_message() {
        error!("no raft message in the first chunk");
        return Err(());
    }

    let meta = head.take_message();
    let key = SnapKey::from_snap(meta.get_message().get_snapshot()).map_err(|e| {
        error!("failed to create snap key: {:?}", e);
    })?;

    let snap = {
        let s = snap_mgr
            .get_snapshot_for_receiving(&key, meta.get_message().get_snapshot().get_data())
            .map_err(|e| {
                error!("{} failed to create snapshot file: {:?}", key, e);
            })?;
        if s.exists() {
            let p = s.path();
            info!("{} snapshot file {} already exists, skip receiving", key, p);
            None
        } else {
            Some(s)
        }
    };

    Ok(RecvSnapContext {
        key: key,
        file: snap,
        raft_msg: meta,
    })
}

pub struct Runner<R: RaftStoreRouter + 'static> {
    env: Arc<Environment>,
    snap_mgr: SnapManager,
    pool: CpuPool,
    raft_router: R,
    security_mgr: Arc<SecurityManager>,
    sending_count: Arc<AtomicUsize>,
    recving_count: Arc<AtomicUsize>,
}

impl<R: RaftStoreRouter + 'static> Runner<R> {
    pub fn new(
        env: Arc<Environment>,
        snap_mgr: SnapManager,
        r: R,
        security_mgr: Arc<SecurityManager>,
    ) -> Runner<R> {
        Runner {
            env: env,
            snap_mgr: snap_mgr,
            pool: CpuPoolBuilder::new()
                .name_prefix(thd_name!("snap sender"))
                .pool_size(DEFAULT_POOL_SIZE)
                .create(),
            raft_router: r,
            security_mgr: security_mgr,
            sending_count: Arc::new(AtomicUsize::new(0)),
            recving_count: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl<R: RaftStoreRouter + 'static> Runnable<Task> for Runner<R> {
    fn run(&mut self, task: Task) {
        match task {
            Task::Recv { stream, sink } => {
                if self.recving_count.load(Ordering::SeqCst) >= MAX_RECEIVER_CONCURRENT {
                    let status = RpcStatus::new(RpcStatusCode::ResourceExhausted, None);
                    self.pool.spawn(sink.fail(status)).forget();
                    return;
                }
                SNAP_TASK_COUNTER.with_label_values(&["recv"]).inc();

                let snap_mgr = self.snap_mgr.clone();
                let raft_router = self.raft_router.clone();
                let recving_count = Arc::clone(&self.recving_count);
                recving_count.fetch_add(1, Ordering::SeqCst);
                let f = recv_snap(stream, sink, snap_mgr, raft_router).then(move |r| {
                    recving_count.fetch_sub(1, Ordering::SeqCst);
                    r
                });
                self.pool.spawn(f).forget();
            }
            Task::SendTo { addr, msg, cb } => {
                if self.sending_count.load(Ordering::SeqCst) >= MAX_SENDER_CONCURRENT {
                    warn!(
                        "too many sending snapshot tasks, drop SendTo Snap[to: {}, snap: {:?}]",
                        addr, msg
                    );
                    cb(Err(Error::Other("Too many sending snapshot tasks".into())));
                    return;
                }
                SNAP_TASK_COUNTER.with_label_values(&["send"]).inc();

                let env = Arc::clone(&self.env);
                let mgr = self.snap_mgr.clone();
                let security_mgr = Arc::clone(&self.security_mgr);
                let sending_count = Arc::clone(&self.sending_count);
                sending_count.fetch_add(1, Ordering::SeqCst);

                let f = future::result(send_snap(env, mgr, security_mgr, &addr, msg))
                    .flatten()
                    .then(move |res| {
                        match res {
                            Ok(stat) => {
                                info!(
                                    "[region {}] sent snapshot {} [size: {}, dur: {:?}]",
                                    stat.key.region_id, stat.key, stat.total_size, stat.elapsed,
                                );
                                cb(Ok(()));
                            }
                            Err(e) => {
                                error!("failed to send snap to {}: {:?}", addr, e);
                                cb(Err(e));
                            }
                        };
                        sending_count.fetch_sub(1, Ordering::SeqCst);
                        future::ok::<_, ()>(())
                    });

                self.pool.spawn(f).forget();
            }
        }
    }
}
