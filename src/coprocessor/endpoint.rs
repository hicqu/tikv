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

use std::{mem, usize};
use std::time::Duration;
use std::cell::RefCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::fmt::{self, Debug, Display, Formatter};

use protobuf::{CodedInputStream, Message as PbMsg};
use futures::{future, stream, Future, Stream};
use futures_cpupool::{Builder as CpuPoolBuilder, CpuPool};
use grpc::{Error as GrpcError, WriteFlags};
#[cfg(test)]
use futures::Sink;

use tipb::select::{self, Chunk, DAGRequest, SelectRequest};
use tipb::analyze::{AnalyzeReq, AnalyzeType};
use tipb::executor::ExecType;
use tipb::schema::ColumnInfo;
use kvproto::coprocessor::{KeyRange, Request, Response};
use kvproto::errorpb::{self, ServerIsBusy};
use kvproto::kvrpcpb::{CommandPri, IsolationLevel};

use util::time::{duration_to_sec, Instant};
use util::worker::{BatchRunnable, FutureScheduler, Scheduler};
use util::collections::HashMap;
use server::{Config, CopResponseSink};
use storage::{self, engine, Engine, FlowStatistics, Snapshot, Statistics, StatisticsSummary};
use storage::engine::Error as EngineError;
use pd::PdTask;

use super::codec::mysql;
use super::codec::datum::Datum;
use super::select::select::SelectContext;
use super::dag::DAGContext;
use super::statistics::analyze::AnalyzeContext;
use super::metrics::*;
use super::{Error, Result};

pub const REQ_TYPE_SELECT: i64 = 101;
pub const REQ_TYPE_INDEX: i64 = 102;
pub const REQ_TYPE_DAG: i64 = 103;
pub const REQ_TYPE_ANALYZE: i64 = 104;
pub const BATCH_ROW_COUNT: usize = 64;
pub const CHUNKS_PER_STREAM: usize = 64;

// If a request has been handled for more than 60 seconds, the client should
// be timeout already, so it can be safely aborted.
const REQUEST_MAX_HANDLE_SECS: u64 = 60;
// If handle time is larger than the lower bound, the query is considered as slow query.
const SLOW_QUERY_LOWER_BOUND: f64 = 1.0; // 1 second.

const DEFAULT_ERROR_CODE: i32 = 1;

pub const SINGLE_GROUP: &'static [u8] = b"SingleGroup";

const OUTDATED_ERROR_MSG: &'static str = "request outdated.";

const ENDPOINT_IS_BUSY: &'static str = "endpoint is busy";

type CopRequestStatistics = RefCell<HashMap<u64, FlowStatistics>>;

struct CopContext {
    select_stats: StatisticsSummary,
    index_stats: StatisticsSummary,
    request_stats: (AtomicBool, CopRequestStatistics),
}

unsafe impl Sync for CopContext {}

impl CopContext {
    fn new() -> Self {
        CopContext {
            select_stats: StatisticsSummary::default(),
            index_stats: StatisticsSummary::default(),
            request_stats: (AtomicBool::new(false), RefCell::new(map![])),
        }
    }

    fn get_statistics(&self, type_str: &str) -> &StatisticsSummary {
        match type_str {
            STR_REQ_TYPE_SELECT => &self.select_stats,
            STR_REQ_TYPE_INDEX => &self.index_stats,
            _ => {
                warn!("unknown STR_REQ_TYPE: {}", type_str);
                &self.select_stats
            }
        }
    }

    fn add_statistics(&self, type_str: &str, stats: &Statistics) {
        self.get_statistics(type_str).add_statistics(stats);
    }

    fn add_statistics_by_region(&self, region_id: u64, stats: &Statistics) {
        let locked = &self.request_stats.0;
        while locked.compare_and_swap(false, true, Ordering::AcqRel) {}
        {
            let mut request_stats = self.request_stats.1.borrow_mut();
            let flow_stats = request_stats
                .entry(region_id)
                .or_insert_with(FlowStatistics::default);
            flow_stats.add(&stats.write.flow_stats);
            flow_stats.add(&stats.data.flow_stats);
        }
        locked.compare_and_swap(true, false, Ordering::AcqRel);
    }
}

pub struct Host {
    engine: Box<Engine>,
    sched: Scheduler<Task>,
    reqs: HashMap<u64, Vec<RequestTask>>,
    last_req_id: u64,
    pool: CpuPool,
    low_priority_pool: CpuPool,
    high_priority_pool: CpuPool,
    max_running_task_count: usize,
    running_task_count: Arc<AtomicUsize>,
    cop_context: Arc<CopContext>,
    pd_task_sender: FutureScheduler<PdTask>,
}

impl Host {
    pub fn new(
        engine: Box<Engine>,
        scheduler: Scheduler<Task>,
        cfg: &Config,
        pd_task_sender: FutureScheduler<PdTask>,
    ) -> Host {
        let create_pool = |name_prefix: &str, size: usize| {
            CpuPoolBuilder::new()
                .name_prefix(name_prefix)
                .pool_size(size)
                .create()
        };

        Host {
            engine: engine,
            sched: scheduler,
            reqs: HashMap::default(),
            last_req_id: 0,
            pool: create_pool("endpoint-normal-pool", cfg.end_point_concurrency),
            low_priority_pool: create_pool("endpoint-low-pool", cfg.end_point_concurrency),
            high_priority_pool: create_pool("endpoint-high-pool", cfg.end_point_concurrency),
            max_running_task_count: cfg.end_point_max_tasks,
            running_task_count: Arc::new(AtomicUsize::new(0)),
            cop_context: Arc::new(CopContext::new()),
            pd_task_sender: pd_task_sender,
        }
    }

    fn running_task_count(&self) -> usize {
        self.running_task_count.load(Ordering::Acquire)
    }

    fn notify_batch_failed_by_id<E: Into<Error> + Debug>(&mut self, e: E, batch_id: u64) {
        debug!("failed to handle batch request: {:?}", e);
        let resp = err_resp(e.into());
        for t in self.reqs.remove(&batch_id).unwrap() {
            t.on_finish.respond(
                resp.clone(),
                &Statistics::default(),
                self.cop_context.clone(),
            );
        }
    }

    fn handle_request_on_sanpshot(&mut self, snap: Box<Snapshot>, mut t: RequestTask) {
        t.on_finish.stop_record_waiting();

        let pri = t.priority();
        let scan_tag = t.ctx.get_scan_tag();
        let pri_str = get_req_pri_str(pri);
        COPR_PENDING_REQS
            .with_label_values(&[scan_tag, pri_str])
            .add(1.0);

        let pool = match pri {
            CommandPri::Low => &mut self.low_priority_pool,
            CommandPri::High => &mut self.high_priority_pool,
            CommandPri::Normal => &mut self.pool,
        };

        let mut statistics = Statistics::default();
        let cop_context = self.cop_context.clone();

        if let Err(e) = t.check_outdated() {
            let future = t.on_finish.on_error(e, &statistics, cop_context);
            pool.spawn(future).forget();
            return;
        }

        let (mut req, cop_req, req_ctx, on_finish) = (t.req, t.cop_req, t.ctx, t.on_finish);
        let ranges = req.take_ranges().into_vec();
        let dispatch_request = move || match cop_req {
            // TODO: here we don't need Arc.
            CopRequest::Select(sel) => {
                let mut ctx = match SelectContext::new(sel, snap, Arc::new(req_ctx)) {
                    Ok(ctx) => ctx,
                    Err(e) => return on_finish.on_error(e, &statistics, cop_context),
                };
                let resp = ctx.handle_request(ranges);
                ctx.collect_statistics_into(&mut statistics);
                match resp {
                    Ok(resp) => on_finish.respond(resp, &statistics, cop_context),
                    Err(e) => on_finish.on_error(e, &statistics, cop_context),
                }
            }
            CopRequest::DAG(dag) => {
                let mut ctx = match DAGContext::new(dag, ranges, snap, Arc::new(req_ctx)) {
                    Ok(ctx) => ctx,
                    Err(e) => return on_finish.on_error(e, &statistics, cop_context),
                };
                if !on_finish.is_streaming() {
                    let resp = ctx.handle_request(false);
                    ctx.collect_statistics_into(&mut statistics);
                    match resp {
                        Ok((resp, _)) => on_finish.respond(resp, &statistics, cop_context),
                        Err(e) => on_finish.on_error(e, &statistics, cop_context),
                    }
                } else {
                    let statistics = Arc::new(statistics);
                    let statistics_in_stream = statistics.clone();
                    let stream = stream::unfold(Some(ctx), move |ctx_opt| {
                        ctx_opt.and_then(|mut ctx| match ctx.handle_request(true) {
                            Ok((resp, true)) => Some(future::ok::<_, _>((resp, Some(ctx)))),
                            Ok((resp, false)) => {
                                ctx.collect_statistics_into(&statistics_in_stream);
                                Some(future::ok::<_, _>((resp, None)))
                            }
                            Err(e) => {
                                ctx.collect_statistics_into(&statistics_in_stream);
                                Some(future::err::<_, _>(e))
                            }
                        })
                    });
                    on_finish.respond_stream(box stream, statistics, cop_context)
                }
            }
            CopRequest::Analyze(analyze) => {
                let ctx = AnalyzeContext::new(analyze, ranges, snap, &req_ctx);
                let resp = ctx.handle_request(&mut statistics);
                match resp {
                    Ok(resp) => on_finish.respond(resp, &statistics, cop_context),
                    Err(e) => on_finish.on_error(e, &statistics, cop_context),
                }
            }
        };
        pool.spawn_fn(dispatch_request).forget();
        COPR_PENDING_REQS
            .with_label_values(&[scan_tag, pri_str])
            .dec()
    }

    fn handle_snapshot_result(&mut self, id: u64, snapshot: engine::Result<Box<Snapshot>>) {
        let snap = match snapshot {
            Ok(s) => s,
            Err(e) => {
                self.notify_batch_failed_by_id(e, id);
                return;
            }
        };

        if self.running_task_count() >= self.max_running_task_count {
            let error = Error::Full(self.max_running_task_count);
            self.notify_batch_failed_by_id(error, id);
            return;
        }

        for req in self.reqs.remove(&id).unwrap() {
            self.handle_request_on_sanpshot(snap.clone(), req);
        }
    }

    fn on_error_quickly(&mut self, req: RequestTask, e: Error) {
        let statistics = Statistics::default();
        let cop_context = self.cop_context.clone();
        self.pool
            .spawn(req.on_finish.on_error(e, &statistics, cop_context))
            .forget();
    }
}

pub enum Task {
    Request(RequestTask),
    SnapRes(u64, engine::Result<Box<Snapshot>>),
    BatchSnapRes(Vec<(u64, engine::Result<Box<Snapshot>>)>),
    RetryRequests(Vec<u64>),
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Task::Request(ref req) => write!(f, "{}", req),
            Task::SnapRes(req_id, _) => write!(f, "snapres [{}]", req_id),
            Task::BatchSnapRes(_) => write!(f, "batch snapres"),
            Task::RetryRequests(ref retry) => write!(f, "retry on task ids: {:?}", retry),
        }
    }
}

#[derive(Debug)]
enum CopRequest {
    Select(SelectRequest),
    DAG(DAGRequest),
    Analyze(AnalyzeReq),
}

#[derive(Debug)]
pub struct ReqContext {
    // The deadline before which the task should be responded.
    pub deadline: Instant,
    pub isolation_level: IsolationLevel,
    pub fill_cache: bool,
    // whether is a table scan request.
    pub table_scan: bool,
}

impl ReqContext {
    #[inline]
    fn get_scan_tag(&self) -> &'static str {
        if self.table_scan {
            STR_REQ_TYPE_SELECT
        } else {
            STR_REQ_TYPE_INDEX
        }
    }

    pub fn check_if_outdated(&self) -> Result<()> {
        let now = Instant::now_coarse();
        if self.deadline <= now {
            return Err(Error::Outdated(self.deadline, now, self.get_scan_tag()));
        }
        Ok(())
    }
}

#[derive(Debug)]
struct OnRequestFinish {
    resp_sink: Option<CopResponseSink>,
    running_task_count: Option<Arc<AtomicUsize>>,
    region_id: u64,
    ranges_len: usize,
    first_range: Option<KeyRange>,
    scan_tag: &'static str,

    timer: Instant,
    wait_time: f64,
    start_ts: u64,
}

impl OnRequestFinish {
    fn is_streaming(&self) -> bool {
        match self.resp_sink.as_ref() {
            Some(&CopResponseSink::Unary(_)) => false,
            Some(&CopResponseSink::Streaming(_)) => true,
            _ => unreachable!(),
        }
    }

    fn attach_task_count(&mut self, running_task_count: Arc<AtomicUsize>) {
        running_task_count.fetch_add(1, Ordering::Release);
        self.running_task_count = Some(running_task_count);
    }

    fn sub_task_count(&mut self) {
        if let Some(count) = self.running_task_count.take() {
            count.fetch_sub(1, Ordering::Release);
        }
    }

    fn respond(
        mut self,
        resp: Response,
        statistics: &Statistics,
        cop_context: Arc<CopContext>,
    ) -> Box<Future<Item = (), Error = GrpcError> + Send> {
        self.sub_task_count();
        self.stop_record_handling(statistics, cop_context);
        match self.resp_sink.take() {
            Some(CopResponseSink::Unary(sink)) => box sink.success(resp),
            Some(CopResponseSink::Streaming(sink)) => {
                let write_flags = WriteFlags::default();
                let stream = stream::once(Ok((resp, write_flags)));
                box stream.forward(sink).map(|_| ())
            }
            #[cfg(test)]
            Some(CopResponseSink::TestChannel(ch)) => box ch.send(resp)
                .map(|_| ())
                .or_else(|_| future::ok::<_, _>(())),
            _ => unreachable!(),
        }
    }

    fn on_error(
        self,
        e: Error,
        statistics: &Statistics,
        cop_context: Arc<CopContext>,
    ) -> Box<Future<Item = (), Error = GrpcError> + Send> {
        let resp = err_resp(e);
        self.respond(resp, statistics, cop_context)
    }

    fn respond_stream(
        mut self,
        stream: Box<Stream<Item = Response, Error = Error> + Send>,
        statistics: Arc<Statistics>,
        cop_context: Arc<CopContext>,
    ) -> Box<Future<Item = (), Error = GrpcError> + Send> {
        let sink = match self.resp_sink.take() {
            Some(CopResponseSink::Streaming(sink)) => sink,
            _ => unreachable!(),
        };
        box stream
            .or_else(|e| future::ok::<_, GrpcError>(err_resp(e)))
            .map(|resp| (resp, WriteFlags::default().buffer_hint(true)))
            .forward(sink)
            .map(move |_| {
                self.sub_task_count();
                self.stop_record_handling(&statistics, cop_context);
            })
    }

    fn stop_record_waiting(&mut self) {
        if self.wait_time > 0f64 {
            return;
        }
        self.wait_time = duration_to_sec(self.timer.elapsed());
        COPR_REQ_WAIT_TIME
            .with_label_values(&[self.scan_tag])
            .observe(self.wait_time);
    }

    fn stop_record_handling(&mut self, statistics: &Statistics, cop_context: Arc<CopContext>) {
        self.stop_record_waiting();
        let handle_time = duration_to_sec(self.timer.elapsed());

        COPR_REQ_HISTOGRAM_VEC
            .with_label_values(&[self.scan_tag])
            .observe(handle_time);

        COPR_REQ_HANDLE_TIME
            .with_label_values(&[self.scan_tag])
            .observe(handle_time - self.wait_time);

        COPR_SCAN_KEYS
            .with_label_values(&[self.scan_tag])
            .observe(statistics.total_op_count() as f64);

        if handle_time > SLOW_QUERY_LOWER_BOUND {
            info!(
                "[region {}] handle {:?} [{}] takes {:?} [waiting: {:?}, keys: {}, hit: {}, \
                 ranges: {} ({:?})]",
                self.region_id,
                self.start_ts,
                self.scan_tag,
                handle_time,
                self.wait_time,
                statistics.total_op_count(),
                statistics.total_processed(),
                self.ranges_len,
                self.first_range,
            );
        }

        cop_context.add_statistics(self.scan_tag, statistics);
        cop_context.add_statistics_by_region(self.region_id, statistics);
    }
}

#[derive(Debug)]
pub struct RequestTask {
    req: Request,
    cop_req: CopRequest,
    ctx: ReqContext,
    on_finish: OnRequestFinish,
}

impl RequestTask {
    /// create a new `RequestTask`. If success, take the `CopResponseSink`
    /// from `resp_sink`, or keep it not changed.
    pub fn new(
        req: Request,
        resp_sink: &mut Option<CopResponseSink>,
        recursion_limit: u32,
    ) -> Result<RequestTask> {
        let table_scan;
        let (start_ts, cop_req) = match req.get_tp() {
            tp @ REQ_TYPE_SELECT | tp @ REQ_TYPE_INDEX => {
                let mut is = CodedInputStream::from_bytes(req.get_data());
                is.set_recursion_limit(recursion_limit);
                let mut sel = SelectRequest::new();
                box_try!(sel.merge_from(&mut is));
                table_scan = tp == REQ_TYPE_SELECT;
                (sel.get_start_ts(), CopRequest::Select(sel))
            }
            REQ_TYPE_DAG => {
                let mut is = CodedInputStream::from_bytes(req.get_data());
                is.set_recursion_limit(recursion_limit);
                let mut dag = DAGRequest::new();
                box_try!(dag.merge_from(&mut is));
                table_scan = dag.get_executors()
                    .iter()
                    .next()
                    .map_or(false, |scan| scan.get_tp() == ExecType::TypeTableScan);
                (dag.get_start_ts(), CopRequest::DAG(dag))
            }
            REQ_TYPE_ANALYZE => {
                let mut is = CodedInputStream::from_bytes(req.get_data());
                is.set_recursion_limit(recursion_limit);
                let mut analyze = AnalyzeReq::new();
                box_try!(analyze.merge_from(&mut is));
                table_scan = analyze.get_tp() == AnalyzeType::TypeColumn;
                (analyze.get_start_ts(), CopRequest::Analyze(analyze))
            }
            tp => return Err(box_err!("unsupported tp {}", tp)),
        };

        let timer = Instant::now_coarse();
        let deadline = timer + Duration::from_secs(REQUEST_MAX_HANDLE_SECS);
        let req_ctx = ReqContext {
            deadline: deadline,
            isolation_level: req.get_context().get_isolation_level(),
            fill_cache: !req.get_context().get_not_fill_cache(),
            table_scan: table_scan,
        };
        let on_finish = OnRequestFinish {
            resp_sink: resp_sink.take(),
            running_task_count: None,
            region_id: req.get_context().get_region_id(),
            ranges_len: req.get_ranges().len(),
            first_range: req.get_ranges().get(0).cloned(),
            scan_tag: req_ctx.get_scan_tag(),

            timer: timer,
            wait_time: 0f64,
            start_ts: start_ts,
        };

        Ok(RequestTask {
            req: req,
            cop_req: cop_req,
            ctx: req_ctx,
            on_finish: on_finish,
        })
    }


    pub fn take_on_finish_sink(&mut self) -> CopResponseSink {
        self.on_finish.resp_sink.take().unwrap()
    }

    #[inline]
    fn check_outdated(&self) -> Result<()> {
        self.ctx.check_if_outdated()
    }


    pub fn priority(&self) -> CommandPri {
        self.req.get_context().get_priority()
    }

    fn get_request_key(&self) -> (u64, u64, u64) {
        let ctx = self.req.get_context();
        let region_id = ctx.get_region_id();
        let version = ctx.get_region_epoch().get_version();
        let peer_id = ctx.get_peer().get_id();
        (region_id, version, peer_id)
    }
}

impl Display for RequestTask {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "request [context {:?}, tp: {}, ranges: {} ({:?})]",
            self.req.get_context(),
            self.req.get_tp(),
            self.req.get_ranges().len(),
            self.req.get_ranges().get(0)
        )
    }
}

impl BatchRunnable<Task> for Host {
    fn run_batch(&mut self, tasks: &mut Vec<Task>) {
        let mut grouped_reqs = map![];
        for task in tasks.drain(..) {
            match task {
                Task::Request(req) => {
                    if let Err(e) = req.check_outdated() {
                        self.on_error_quickly(req, e);
                        continue;
                    }
                    let key = req.get_request_key();
                    grouped_reqs.entry(key).or_insert_with(Vec::new).push(req);
                }
                Task::SnapRes(q_id, snap_res) => {
                    self.handle_snapshot_result(q_id, snap_res);
                }
                Task::BatchSnapRes(batch) => for (q_id, snap_res) in batch {
                    self.handle_snapshot_result(q_id, snap_res);
                },
                Task::RetryRequests(retry) => for id in retry {
                    if let Err(e) = {
                        let ctx = self.reqs[&id][0].req.get_context();
                        let sched = self.sched.clone();
                        self.engine.async_snapshot(ctx, box move |(_, res)| {
                            sched.schedule(Task::SnapRes(id, res)).unwrap()
                        })
                    } {
                        self.notify_batch_failed_by_id(e, id);
                    }
                },
            }
        }

        if grouped_reqs.is_empty() {
            return;
        }

        let mut batch = Vec::with_capacity(grouped_reqs.len());
        let start_id = self.last_req_id + 1;
        for (_, mut reqs) in grouped_reqs {
            let max_running_task_count = self.max_running_task_count;
            if self.running_task_count() >= max_running_task_count {
                for req in reqs {
                    self.on_error_quickly(req, Error::Full(max_running_task_count));
                }
                continue;
            }

            for req in &mut reqs {
                let running_task_count = self.running_task_count.clone();
                req.on_finish.attach_task_count(running_task_count);
            }

            let id = self.last_req_id;
            batch.push(reqs[0].req.get_context().clone());
            self.reqs.insert(id, reqs);
            self.last_req_id += 1;
        }
        let end_id = self.last_req_id;

        let sched = self.sched.clone();
        let on_finished: engine::BatchCallback<Box<Snapshot>> = box move |results: Vec<_>| {
            let mut ready = Vec::with_capacity(results.len());
            let mut retry = Vec::new();
            for (id, res) in (start_id..end_id + 1).zip(results) {
                match res {
                    Some((_, res)) => ready.push((id, res)),
                    None => retry.push(id),
                }
            }

            if !ready.is_empty() {
                sched.schedule(Task::BatchSnapRes(ready)).unwrap();
            }
            if !retry.is_empty() {
                BATCH_REQUEST_TASKS
                    .with_label_values(&["retry"])
                    .observe(retry.len() as f64);
                sched.schedule(Task::RetryRequests(retry)).unwrap();
            }
        };

        BATCH_REQUEST_TASKS
            .with_label_values(&["all"])
            .observe(batch.len() as f64);

        if let Err(e) = self.engine.async_batch_snapshot(batch, on_finished) {
            for id in start_id..end_id + 1 {
                let err = e.maybe_clone().unwrap_or_else(|| {
                    error!("async snapshot batch failed error {:?}", e);
                    EngineError::Other(box_err!("{:?}", e))
                });
                self.notify_batch_failed_by_id(err, id);
            }
        }
    }

    fn run_periodic(&mut self) {
        for type_str in &[STR_REQ_TYPE_SELECT, STR_REQ_TYPE_INDEX] {
            let this_statistics = self.cop_context.get_statistics(type_str);
            if atomic_load!(this_statistics.count) == 0 {
                continue;
            }
            for (cf, details) in this_statistics.stat.take_details() {
                for (tag, count) in details {
                    COPR_SCAN_DETAILS
                        .with_label_values(&[type_str, cf, tag])
                        .inc_by(count as f64)
                        .unwrap();
                }
            }
            this_statistics.count.store(0, Ordering::Release);
        }

        let mut to_send_stats = HashMap::default();

        let locked = &self.cop_context.request_stats.0;
        while locked.compare_and_swap(false, true, Ordering::AcqRel) {}
        {
            let mut request_stats = self.cop_context.request_stats.1.borrow_mut();
            if !request_stats.is_empty() {
                mem::swap(&mut to_send_stats, &mut request_stats);
            }
        }
        locked.compare_and_swap(true, false, Ordering::AcqRel);

        if !to_send_stats.is_empty() {
            if let Err(e) = self.pd_task_sender.schedule(PdTask::ReadStats {
                read_stats: to_send_stats,
            }) {
                error!("send coprocessor statistics: {:?}", e);
            }
        }
    }
}

fn err_resp(e: Error) -> Response {
    let mut resp = Response::new();
    match e {
        Error::Region(e) => {
            let tag = storage::get_tag_from_header(&e);
            COPR_REQ_ERROR.with_label_values(&[tag]).inc();
            resp.set_region_error(e);
        }
        Error::Locked(info) => {
            resp.set_locked(info);
            COPR_REQ_ERROR.with_label_values(&["lock"]).inc();
        }
        Error::Outdated(deadline, now, scan_tag) => {
            let elapsed =
                now.duration_since(deadline) + Duration::from_secs(REQUEST_MAX_HANDLE_SECS);
            COPR_REQ_ERROR.with_label_values(&["outdated"]).inc();
            OUTDATED_REQ_WAIT_TIME
                .with_label_values(&[scan_tag])
                .observe(elapsed.as_secs() as f64);

            resp.set_other_error(OUTDATED_ERROR_MSG.to_owned());
        }
        Error::Full(allow) => {
            COPR_REQ_ERROR.with_label_values(&["full"]).inc();
            let mut errorpb = errorpb::Error::new();
            errorpb.set_message(format!("running batches reach limit {}", allow));
            let mut server_is_busy_err = ServerIsBusy::new();
            server_is_busy_err.set_reason(ENDPOINT_IS_BUSY.to_owned());
            errorpb.set_server_is_busy(server_is_busy_err);
            resp.set_region_error(errorpb);
        }
        Error::Other(_) => {
            resp.set_other_error(format!("{}", e));
            COPR_REQ_ERROR.with_label_values(&["other"]).inc();
        }
    }
    resp
}

pub fn to_pb_error(err: &Error) -> select::Error {
    let mut e = select::Error::new();
    e.set_code(DEFAULT_ERROR_CODE);
    e.set_msg(format!("{}", err));
    e
}

pub fn prefix_next(key: &[u8]) -> Vec<u8> {
    let mut nk = key.to_vec();
    if nk.is_empty() {
        nk.push(0);
        return nk;
    }
    let mut i = nk.len() - 1;
    loop {
        if nk[i] == 255 {
            nk[i] = 0;
        } else {
            nk[i] += 1;
            return nk;
        }
        if i == 0 {
            nk = key.to_vec();
            nk.push(0);
            return nk;
        }
        i -= 1;
    }
}

/// `is_point` checks if the key range represents a point.
pub fn is_point(range: &KeyRange) -> bool {
    range.get_end() == &*prefix_next(range.get_start())
}

#[inline]
pub fn get_pk(col: &ColumnInfo, h: i64) -> Datum {
    if mysql::has_unsigned_flag(col.get_flag() as u64) {
        // PK column is unsigned
        Datum::U64(h as u64)
    } else {
        Datum::I64(h)
    }
}

#[inline]
pub fn get_chunk(chunks: &mut Vec<Chunk>) -> &mut Chunk {
    if chunks
        .last()
        .map_or(true, |chunk| chunk.get_rows_meta().len() >= BATCH_ROW_COUNT)
    {
        let chunk = Chunk::new();
        chunks.push(chunk);
    }
    chunks.last_mut().unwrap()
}

pub const STR_REQ_TYPE_SELECT: &'static str = "select";
pub const STR_REQ_TYPE_INDEX: &'static str = "index";
pub const STR_REQ_PRI_LOW: &'static str = "low";
pub const STR_REQ_PRI_NORMAL: &'static str = "normal";
pub const STR_REQ_PRI_HIGH: &'static str = "high";

#[inline]
pub fn get_req_pri_str(pri: CommandPri) -> &'static str {
    match pri {
        CommandPri::Low => STR_REQ_PRI_LOW,
        CommandPri::Normal => STR_REQ_PRI_NORMAL,
        CommandPri::High => STR_REQ_PRI_HIGH,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use storage::engine::{self, TEMP_DIR};
    use std::time::Duration;

    use futures::{future, Future, Stream};
    use futures::sync::mpsc;
    use tokio_timer::wheel;

    use kvproto::coprocessor::Request;
    use tipb::select::DAGRequest;
    use tipb::expression::Expr;
    use tipb::executor::Executor;

    use util::worker::{FutureWorker, Worker};
    use util::time::Instant;

    #[test]
    fn test_get_reg_scan_tag() {
        let mut ctx = ReqContext {
            deadline: Instant::now_coarse(),
            isolation_level: IsolationLevel::RC,
            fill_cache: true,
            table_scan: true,
        };
        assert_eq!(ctx.get_scan_tag(), STR_REQ_TYPE_SELECT);
        ctx.table_scan = false;
        assert_eq!(ctx.get_scan_tag(), STR_REQ_TYPE_INDEX);
    }

    #[test]
    fn test_req_outdated() {
        let mut worker = Worker::new("test-endpoint");
        let engine = engine::new_local_engine(TEMP_DIR, &[]).unwrap();
        let mut cfg = Config::default();
        cfg.end_point_concurrency = 1;
        let pd_worker = FutureWorker::new("test-pd-worker");
        let end_point = Host::new(engine, worker.scheduler(), &cfg, pd_worker.scheduler());
        worker.start_batch(end_point, 30).unwrap();

        let mut req = Request::new();
        req.set_tp(REQ_TYPE_DAG);

        let (tx, mut rx) = mpsc::channel(1);
        let mut cop_resp_sink_opt = Some(CopResponseSink::TestChannel(tx));
        let mut task = RequestTask::new(req, &mut cop_resp_sink_opt, 1000).unwrap();
        task.ctx = ReqContext {
            deadline: task.ctx.deadline - Duration::from_secs(super::REQUEST_MAX_HANDLE_SECS),
            isolation_level: task.ctx.isolation_level,
            fill_cache: task.ctx.fill_cache,
            table_scan: task.ctx.table_scan,
        };
        worker.schedule(Task::Request(task)).unwrap();

        let resp = wheel()
            .build()
            .timeout(future::poll_fn(|| rx.poll()), Duration::from_secs(3))
            .wait()
            .unwrap()
            .unwrap();
        assert!(!resp.get_other_error().is_empty());
        assert_eq!(resp.get_other_error(), super::OUTDATED_ERROR_MSG);
    }

    #[test]
    fn test_too_many_reqs() {
        let mut worker = Worker::new("test-endpoint");
        let engine = engine::new_local_engine(TEMP_DIR, &[]).unwrap();
        let mut cfg = Config::default();
        cfg.end_point_concurrency = 1;
        let pd_worker = FutureWorker::new("test-pd-worker");
        let mut end_point = Host::new(engine, worker.scheduler(), &cfg, pd_worker.scheduler());
        end_point.max_running_task_count = 3;
        worker.start_batch(end_point, 30).unwrap();

        let mut rx_vec = Vec::with_capacity(120);
        for pos in 0..30 * 4 {
            let mut req = Request::new();
            req.set_tp(REQ_TYPE_DAG);
            req.mut_context().set_region_id(pos % 5);
            if pos % 3 == 0 {
                req.mut_context().set_priority(CommandPri::Low);
            } else if pos % 3 == 1 {
                req.mut_context().set_priority(CommandPri::Normal);
            } else {
                req.mut_context().set_priority(CommandPri::High);
            }

            let (tx, rx) = mpsc::channel(1);
            let mut cop_resp_sink_opt = Some(CopResponseSink::TestChannel(tx));
            let task = RequestTask::new(req, &mut cop_resp_sink_opt, 1000).unwrap();
            worker.schedule(Task::Request(task)).unwrap();
            rx_vec.push(rx);
        }
        for i in 0..120 {
            let resp = wheel()
                .build()
                .timeout(future::poll_fn(|| rx_vec[i].poll()), Duration::from_secs(3))
                .wait()
                .unwrap()
                .unwrap();
            if !resp.has_region_error() {
                continue;
            }
            assert!(resp.get_region_error().has_server_is_busy());
            return;
        }
        panic!("suppose to get ServerIsBusy error.");
    }

    #[test]
    fn test_stack_guard() {
        let mut expr = Expr::new();
        for _ in 0..10 {
            let mut e = Expr::new();
            e.mut_children().push(expr);
            expr = e;
        }
        let mut e = Executor::new();
        e.mut_selection().mut_conditions().push(expr);
        let mut dag = DAGRequest::new();
        dag.mut_executors().push(e);
        let mut req = Request::new();
        req.set_tp(REQ_TYPE_DAG);
        req.set_data(dag.write_to_bytes().unwrap());

        let (tx, _) = mpsc::channel(10);
        let mut cop_resp_sink_opt = Some(CopResponseSink::TestChannel(tx));
        let err = RequestTask::new(req, &mut cop_resp_sink_opt, 5).unwrap_err();
        let s = format!("{:?}", err);
        assert!(
            s.contains("Recursion"),
            "parse should fail due to recursion limit {}",
            s
        );
        assert!(cop_resp_sink_opt.is_some());
    }
}
