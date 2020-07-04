// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use engine_rocks::{RocksEngine, RocksSnapshot, RocksTablePropertiesCollection};
use engine_traits::IterOptions;
use engine_traits::{CfName, DeleteStrategy};
use engine_traits::{Iterable, MiscExt, Peekable, TablePropertiesExt};
use engine_traits::{CF_DEFAULT, CF_LOCK, CF_RAFT};
use kvproto::errorpb;
use kvproto::kvrpcpb::Context;
use kvproto::raft_cmdpb::{
    CmdType, DeleteRangeRequest, DeleteRequest, PutRequest, RaftCmdRequest, RaftCmdResponse,
    RaftRequestHeader, Request, Response,
};
use kvproto::raft_serverpb::RegionLocalState;
use std::fmt::{self, Debug, Display, Formatter};
use std::io::Error as IoError;
use std::result;
use std::time::Duration;
use txn_types::{Key, Value};

use super::metrics::*;
use crate::storage::kv::{
    Callback, CbContext, Cursor, Engine, Error as KvError, ErrorInner as KvErrorInner,
    Iterator as EngineIterator, Modify, ScanMode, Snapshot, WriteData,
};
use crate::storage::{self, kv};
use raftstore::errors::Error as RaftServerError;
use raftstore::router::RaftStoreRouter;
use raftstore::store::msg::StoreMsg;
use raftstore::store::{Callback as StoreCallback, ReadResponse, WriteResponse};
use raftstore::store::{RegionIterator, RegionSnapshot};
use tikv_util::time::Instant;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        RequestFailed(e: errorpb::Error) {
            from()
            display("{}", e.get_message())
        }
        Io(e: IoError) {
            from()
            cause(e)
            display("{}", e)
        }

        Server(e: RaftServerError) {
            from()
            cause(e)
            display("{}", e)
        }
        InvalidResponse(reason: String) {
            display("{}", reason)
        }
        InvalidRequest(reason: String) {
            display("{}", reason)
        }
        Timeout(d: Duration) {
            display("timeout after {:?}", d)
        }
    }
}

fn get_status_kind_from_error(e: &Error) -> RequestStatusKind {
    match *e {
        Error::RequestFailed(ref header) => {
            RequestStatusKind::from(storage::get_error_kind_from_header(header))
        }
        Error::Io(_) => RequestStatusKind::err_io,
        Error::Server(_) => RequestStatusKind::err_server,
        Error::InvalidResponse(_) => RequestStatusKind::err_invalid_resp,
        Error::InvalidRequest(_) => RequestStatusKind::err_invalid_req,
        Error::Timeout(_) => RequestStatusKind::err_timeout,
    }
}

fn get_status_kind_from_engine_error(e: &kv::Error) -> RequestStatusKind {
    match *e {
        KvError(box KvErrorInner::Request(ref header)) => {
            RequestStatusKind::from(storage::get_error_kind_from_header(header))
        }

        KvError(box KvErrorInner::Timeout(_)) => RequestStatusKind::err_timeout,
        KvError(box KvErrorInner::EmptyRequest) => RequestStatusKind::err_empty_request,
        KvError(box KvErrorInner::Other(_)) => RequestStatusKind::err_other,
    }
}

pub type Result<T> = result::Result<T, Error>;

impl From<Error> for kv::Error {
    fn from(e: Error) -> kv::Error {
        match e {
            Error::RequestFailed(e) => KvError::from(KvErrorInner::Request(e)),
            Error::Server(e) => e.into(),
            e => box_err!(e),
        }
    }
}

impl From<RaftServerError> for KvError {
    fn from(e: RaftServerError) -> KvError {
        KvError(Box::new(KvErrorInner::Request(e.into())))
    }
}

/// `RaftKv` is a storage engine base on `RaftStore`.
#[derive(Clone)]
pub struct RaftKv<S: RaftStoreRouter<RocksSnapshot> + 'static> {
    router: S,
    engine: RocksEngine,
}

pub enum CmdRes {
    Resp(Vec<Response>),
    Snap(RegionSnapshot<RocksSnapshot>),
}

fn new_ctx(resp: &RaftCmdResponse) -> CbContext {
    let mut cb_ctx = CbContext::new();
    cb_ctx.term = Some(resp.get_header().get_current_term());
    cb_ctx
}

fn check_raft_cmd_response(resp: &mut RaftCmdResponse, req_cnt: usize) -> Result<()> {
    if resp.get_header().has_error() {
        return Err(Error::RequestFailed(resp.take_header().take_error()));
    }
    if req_cnt != resp.get_responses().len() {
        return Err(Error::InvalidResponse(format!(
            "responses count {} is not equal to requests count {}",
            resp.get_responses().len(),
            req_cnt
        )));
    }

    Ok(())
}

fn on_write_result(mut write_resp: WriteResponse, req_cnt: usize) -> (CbContext, Result<CmdRes>) {
    let cb_ctx = new_ctx(&write_resp.response);
    if let Err(e) = check_raft_cmd_response(&mut write_resp.response, req_cnt) {
        return (cb_ctx, Err(e));
    }
    let resps = write_resp.response.take_responses();
    (cb_ctx, Ok(CmdRes::Resp(resps.into())))
}

fn on_read_result(
    mut read_resp: ReadResponse<RocksSnapshot>,
    req_cnt: usize,
) -> (CbContext, Result<CmdRes>) {
    // TODO(5kbpers): set ExtraOp for cb_ctx here.
    let cb_ctx = new_ctx(&read_resp.response);
    if let Err(e) = check_raft_cmd_response(&mut read_resp.response, req_cnt) {
        return (cb_ctx, Err(e));
    }
    let resps = read_resp.response.take_responses();
    if !resps.is_empty() || resps[0].get_cmd_type() == CmdType::Snap {
        (cb_ctx, Ok(CmdRes::Snap(read_resp.snapshot.unwrap())))
    } else {
        (cb_ctx, Ok(CmdRes::Resp(resps.into())))
    }
}

impl<S: RaftStoreRouter<RocksSnapshot>> RaftKv<S> {
    /// Create a RaftKv using specified configuration.
    pub fn new(router: S, engine: RocksEngine) -> RaftKv<S> {
        RaftKv { router, engine }
    }

    fn new_request_header(&self, ctx: &Context) -> RaftRequestHeader {
        let mut header = RaftRequestHeader::default();
        header.set_region_id(ctx.get_region_id());
        header.set_peer(ctx.get_peer().clone());
        header.set_region_epoch(ctx.get_region_epoch().clone());
        if ctx.get_term() != 0 {
            header.set_term(ctx.get_term());
        }
        header.set_sync_log(ctx.get_sync_log());
        header.set_replica_read(ctx.get_replica_read());
        header
    }

    fn exec_read_requests(
        &self,
        ctx: &Context,
        reqs: Vec<Request>,
        cb: Callback<CmdRes>,
    ) -> Result<()> {
        let len = reqs.len();
        let header = self.new_request_header(ctx);
        let mut cmd = RaftCmdRequest::default();
        cmd.set_header(header);
        cmd.set_requests(reqs.into());

        self.router
            .send_command(
                cmd,
                StoreCallback::Read(Box::new(move |resp| {
                    let (cb_ctx, res) = on_read_result(resp, len);
                    cb((cb_ctx, res.map_err(Error::into)));
                })),
            )
            .map_err(From::from)
    }

    fn exec_write_requests(
        &self,
        ctx: &Context,
        reqs: Vec<Request>,
        cb: Callback<CmdRes>,
    ) -> Result<()> {
        #[cfg(feature = "failpoints")]
        {
            // If rid is some, only the specified region reports error.
            // If rid is None, all regions report error.
            let raftkv_early_error_report_fp = || -> Result<()> {
                fail_point!("raftkv_early_error_report", |rid| {
                    let region_id = ctx.get_region_id();
                    rid.and_then(|rid| {
                        let rid: u64 = rid.parse().unwrap();
                        if rid == region_id {
                            None
                        } else {
                            Some(())
                        }
                    })
                    .ok_or_else(|| RaftServerError::RegionNotFound(region_id).into())
                });
                Ok(())
            };
            raftkv_early_error_report_fp()?;
        }

        let len = reqs.len();
        let header = self.new_request_header(ctx);
        let mut cmd = RaftCmdRequest::default();
        cmd.set_header(header);
        cmd.set_requests(reqs.into());

        self.router
            .send_command(
                cmd,
                StoreCallback::Write(Box::new(move |resp| {
                    let (cb_ctx, res) = on_write_result(resp, len);
                    cb((cb_ctx, res.map_err(Error::into)));
                })),
            )
            .map_err(From::from)
    }
}

fn invalid_resp_type(exp: CmdType, act: CmdType) -> Error {
    Error::InvalidResponse(format!(
        "cmd type not match, want {:?}, got {:?}!",
        exp, act
    ))
}

impl<S: RaftStoreRouter<RocksSnapshot>> Display for RaftKv<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "RaftKv")
    }
}

impl<S: RaftStoreRouter<RocksSnapshot>> Debug for RaftKv<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "RaftKv")
    }
}

impl<S: RaftStoreRouter<RocksSnapshot>> Engine for RaftKv<S> {
    type Snap = RegionSnapshot<RocksSnapshot>;

    fn async_write(&self, ctx: &Context, batch: WriteData, cb: Callback<()>) -> kv::Result<()> {
        fail_point!("raftkv_async_write");
        if batch.modifies.is_empty() {
            return Err(KvError::from(KvErrorInner::EmptyRequest));
        }

        let mut reqs = Vec::with_capacity(batch.modifies.len());
        for m in batch.modifies {
            let mut req = Request::default();
            match m {
                Modify::Delete(cf, k) => {
                    let mut delete = DeleteRequest::default();
                    delete.set_key(k.into_encoded());
                    if cf != CF_DEFAULT {
                        delete.set_cf(cf.to_string());
                    }
                    req.set_cmd_type(CmdType::Delete);
                    req.set_delete(delete);
                }
                Modify::Put(cf, k, v) => {
                    let mut put = PutRequest::default();
                    put.set_key(k.into_encoded());
                    put.set_value(v);
                    if cf != CF_DEFAULT {
                        put.set_cf(cf.to_string());
                    }
                    req.set_cmd_type(CmdType::Put);
                    req.set_put(put);
                }
                Modify::DeleteRange(cf, start_key, end_key, notify_only) => {
                    let mut delete_range = DeleteRangeRequest::default();
                    delete_range.set_cf(cf.to_string());
                    delete_range.set_start_key(start_key.into_encoded());
                    delete_range.set_end_key(end_key.into_encoded());
                    delete_range.set_notify_only(notify_only);
                    req.set_cmd_type(CmdType::DeleteRange);
                    req.set_delete_range(delete_range);
                }
            }
            reqs.push(req);
        }

        ASYNC_REQUESTS_COUNTER_VEC.write.all.inc();
        let begin_instant = Instant::now_coarse();

        // TODO(5kbpers): send WriteData::TxnExtra to raftstore here.
        self.exec_write_requests(
            ctx,
            reqs,
            Box::new(move |(cb_ctx, res)| match res {
                Ok(CmdRes::Resp(_)) => {
                    ASYNC_REQUESTS_COUNTER_VEC.write.success.inc();
                    ASYNC_REQUESTS_DURATIONS_VEC
                        .write
                        .observe(begin_instant.elapsed_secs());
                    fail_point!("raftkv_async_write_finish");
                    cb((cb_ctx, Ok(())))
                }
                Ok(CmdRes::Snap(_)) => cb((
                    cb_ctx,
                    Err(box_err!("unexpect snapshot, should mutate instead.")),
                )),
                Err(e) => {
                    let status_kind = get_status_kind_from_engine_error(&e);
                    ASYNC_REQUESTS_COUNTER_VEC.write.get(status_kind).inc();
                    cb((cb_ctx, Err(e)))
                }
            }),
        )
        .map_err(|e| {
            let status_kind = get_status_kind_from_error(&e);
            ASYNC_REQUESTS_COUNTER_VEC.write.get(status_kind).inc();
            e.into()
        })
    }

    fn async_snapshot(&self, ctx: &Context, cb: Callback<Self::Snap>) -> kv::Result<()> {
        fail_point!("raftkv_async_snapshot");
        let mut req = Request::default();
        req.set_cmd_type(CmdType::Snap);

        ASYNC_REQUESTS_COUNTER_VEC.snapshot.all.inc();
        let begin_instant = Instant::now_coarse();

        self.exec_read_requests(
            ctx,
            vec![req],
            Box::new(move |(cb_ctx, res)| match res {
                Ok(CmdRes::Resp(r)) => cb((
                    cb_ctx,
                    Err(invalid_resp_type(CmdType::Snap, r[0].get_cmd_type()).into()),
                )),
                Ok(CmdRes::Snap(s)) => {
                    ASYNC_REQUESTS_DURATIONS_VEC
                        .snapshot
                        .observe(begin_instant.elapsed_secs());
                    ASYNC_REQUESTS_COUNTER_VEC.snapshot.success.inc();
                    cb((cb_ctx, Ok(s)))
                }
                Err(e) => {
                    let status_kind = get_status_kind_from_engine_error(&e);
                    ASYNC_REQUESTS_COUNTER_VEC.snapshot.get(status_kind).inc();
                    cb((cb_ctx, Err(e)))
                }
            }),
        )
        .map_err(|e| {
            let status_kind = get_status_kind_from_error(&e);
            ASYNC_REQUESTS_COUNTER_VEC.snapshot.get(status_kind).inc();
            e.into()
        })
    }

    fn get_properties_cf(
        &self,
        cf: CfName,
        start: &[u8],
        end: &[u8],
    ) -> kv::Result<RocksTablePropertiesCollection> {
        let start = keys::data_key(start);
        let end = keys::data_end_key(end);
        self.engine
            .get_range_properties_cf(cf, &start, &end)
            .map_err(|e| e.into())
    }

    fn delete_all_in_range_cf(
        &self,
        cf: &str,
        strategy: DeleteStrategy,
        start_key: &[u8],
        end_key: &[u8],
    ) -> kv::Result<usize> {
        self.engine
            .delete_all_in_range_cf(cf, strategy, start_key, end_key)
            .map_err(|e| e.into())
    }

    fn delete_files_in_range_cf(
        &self,
        cf: &str,
        start_key: &[u8],
        end_key: &[u8],
    ) -> kv::Result<()> {
        let mut region_ranges = vec![];
        let start_data_key = start_key.to_vec();
        let end_data_key = end_key.to_vec();
        if cf != CF_LOCK {
            let ret = self
                .engine
                .scan_cf(
                    CF_RAFT,
                    keys::REGION_META_MIN_KEY,
                    keys::REGION_META_MAX_KEY,
                    false,
                    |key, value| {
                        let (_, suffix) = box_try!(keys::decode_region_meta_key(key));
                        if suffix != keys::REGION_STATE_SUFFIX {
                            return Ok(true);
                        }
                        let local_state = protobuf::parse_from_bytes::<RegionLocalState>(value)?;
                        let region = local_state.get_region();
                        if region.get_peers().is_empty() {
                            return Ok(true);
                        }

                        let key = keys::enc_end_key(region);
                        if key > start_data_key && key < end_data_key {
                            region_ranges.push(key);
                        }
                        Ok(true)
                    },
                )
                .map_err(|e| e.into());
            if ret.is_err() {
                return ret;
            }
        }
        region_ranges.sort();
        const DELETE_REGION_LIMIT: usize = 8;
        let l = region_ranges.len() / DELETE_REGION_LIMIT;
        for i in 1..l {
            // Ignore error because we will delete it again.
            let _ = self.engine.delete_files_in_range_cf(
                cf,
                start_key,
                &region_ranges[i * DELETE_REGION_LIMIT],
                false,
            );
        }
        info!(
                "unsafe destroy range finished deleting regions in range";
                "region_count" => region_ranges.len()
        );
        self.engine
            .delete_files_in_range_cf(cf, start_key, end_key, false)
            .map_err(|e| e.into())
    }

    fn unsafe_destroy_range(&self, sst_path: String, start: &Key, end: &Key) -> kv::Result<()> {
        self.unsafe_destroy_range_impl(sst_path, start, end)?;
        self.router
            .send_store(StoreMsg::ClearRegionSizeInRange {
                start_key: start.as_encoded().to_vec(),
                end_key: end.as_encoded().to_vec(),
            })
            .unwrap_or_else(|e| {
                warn!(
                    "unsafe destroy range: failed sending ClearRegionSizeInRange";
                    "err" => ?e
                );
            });
        Ok(())
    }
}

impl Snapshot for RegionSnapshot<RocksSnapshot> {
    type Iter = RegionIterator<RocksSnapshot>;

    fn get(&self, key: &Key) -> kv::Result<Option<Value>> {
        fail_point!("raftkv_snapshot_get", |_| Err(box_err!(
            "injected error for get"
        )));
        let v = box_try!(self.get_value(key.as_encoded()));
        Ok(v.map(|v| v.to_vec()))
    }

    fn get_cf(&self, cf: CfName, key: &Key) -> kv::Result<Option<Value>> {
        fail_point!("raftkv_snapshot_get_cf", |_| Err(box_err!(
            "injected error for get_cf"
        )));
        let v = box_try!(self.get_value_cf(cf, key.as_encoded()));
        Ok(v.map(|v| v.to_vec()))
    }

    fn iter(&self, iter_opt: IterOptions, mode: ScanMode) -> kv::Result<Cursor<Self::Iter>> {
        fail_point!("raftkv_snapshot_iter", |_| Err(box_err!(
            "injected error for iter"
        )));
        Ok(Cursor::new(RegionSnapshot::iter(self, iter_opt), mode))
    }

    fn iter_cf(
        &self,
        cf: CfName,
        iter_opt: IterOptions,
        mode: ScanMode,
    ) -> kv::Result<Cursor<Self::Iter>> {
        fail_point!("raftkv_snapshot_iter_cf", |_| Err(box_err!(
            "injected error for iter_cf"
        )));
        Ok(Cursor::new(
            RegionSnapshot::iter_cf(self, cf, iter_opt)?,
            mode,
        ))
    }

    #[inline]
    fn lower_bound(&self) -> Option<&[u8]> {
        Some(self.get_start_key())
    }

    #[inline]
    fn upper_bound(&self) -> Option<&[u8]> {
        Some(self.get_end_key())
    }

    #[inline]
    fn get_data_version(&self) -> Option<u64> {
        self.get_apply_index().ok()
    }
}

impl EngineIterator for RegionIterator<RocksSnapshot> {
    fn next(&mut self) -> kv::Result<bool> {
        RegionIterator::next(self).map_err(KvError::from)
    }

    fn prev(&mut self) -> kv::Result<bool> {
        RegionIterator::prev(self).map_err(KvError::from)
    }

    fn seek(&mut self, key: &Key) -> kv::Result<bool> {
        fail_point!("raftkv_iter_seek", |_| Err(box_err!(
            "injected error for iter_seek"
        )));
        RegionIterator::seek(self, key.as_encoded()).map_err(From::from)
    }

    fn seek_for_prev(&mut self, key: &Key) -> kv::Result<bool> {
        fail_point!("raftkv_iter_seek_for_prev", |_| Err(box_err!(
            "injected error for iter_seek_for_prev"
        )));
        RegionIterator::seek_for_prev(self, key.as_encoded()).map_err(From::from)
    }

    fn seek_to_first(&mut self) -> kv::Result<bool> {
        RegionIterator::seek_to_first(self).map_err(KvError::from)
    }

    fn seek_to_last(&mut self) -> kv::Result<bool> {
        RegionIterator::seek_to_last(self).map_err(KvError::from)
    }

    fn valid(&self) -> kv::Result<bool> {
        RegionIterator::valid(self).map_err(KvError::from)
    }

    fn validate_key(&self, key: &Key) -> kv::Result<()> {
        self.should_seekable(key.as_encoded()).map_err(From::from)
    }

    fn key(&self) -> &[u8] {
        RegionIterator::key(self)
    }

    fn value(&self) -> &[u8] {
        RegionIterator::value(self)
    }
}
