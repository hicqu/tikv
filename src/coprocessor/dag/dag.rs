// Copyright 2017 PingCAP, Inc.
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

use std::sync::Arc;

use tipb::schema::ColumnInfo;
use tipb::select::{Chunk, DAGRequest, SelectResponse};
use kvproto::coprocessor::{KeyRange, Response};
use protobuf::Message as PbMsg;

use coprocessor::codec::mysql;
use coprocessor::codec::datum::{Datum, DatumEncoder};
use coprocessor::select::xeval::EvalContext;
use coprocessor::{Error, Result};
use coprocessor::endpoint::{get_pk, to_pb_error, ReqContext, BATCH_ROW_COUNT, CHUNKS_PER_STREAM};
use storage::{Snapshot, SnapshotStore, Statistics};

use super::executor::{build_exec, Executor, Row};

pub struct DAGContext {
    columns: Arc<Vec<ColumnInfo>>,
    has_aggr: bool,
    req_ctx: Arc<ReqContext>,
    exec: Box<Executor>,
    output_offsets: Vec<u32>,
    streaming_chunks: Vec<Chunks>,
}

impl DAGContext {
    pub fn new(
        mut req: DAGRequest,
        ranges: Vec<KeyRange>,
        snap: Box<Snapshot>,
        req_ctx: Arc<ReqContext>,
    ) -> Result<DAGContext> {
        let eval_ctx = Arc::new(box_try!(EvalContext::new(
            req.get_time_zone_offset(),
            req.get_flags()
        )));
        let store = SnapshotStore::new(
            snap,
            req.get_start_ts(),
            req_ctx.isolation_level,
            req_ctx.fill_cache,
        );

        let dag_executor = build_exec(req.take_executors().into_vec(), store, ranges, eval_ctx)?;
        Ok(DAGContext {
            columns: dag_executor.columns,
            has_aggr: dag_executor.has_aggr,
            req_ctx: req_ctx,
            exec: dag_executor.exec,
            output_offsets: req.take_output_offsets(),
        })
    }

    pub fn handle_request(&mut self, streaming: bool) -> Result<(Response, bool)> {
        let mut chunk = Chunk::default();
        let mut cur_row_count = 0;
        loop {
            match self.exec.next() {
                Ok(Some(row)) => {
                    self.req_ctx.check_if_outdated()?;
                    if self.has_aggr {
                        chunk.mut_rows_data().extend_from_slice(&row.data.value);
                    } else {
                        let value = inflate_cols(&row, &self.columns, &self.output_offsets)?;
                        chunk.mut_rows_data().extend_from_slice(&value);
                    }
                    cur_row_count += 1;
                    if streaming && cur_row_count >= BATCH_ROW_COUNT {
                        self.streaming_chunks.push(chunk);
                        if self.streaming_chunks.len() >= CHUNKS_PER_STREAM {
                            
                            self.streaming_chunks;
                            return 
                        }
                        return response_from_chunk(chunk, true);
                    }
                }
                Ok(None) => return response_from_chunk(chunk, cur_row_count > 0),
                Err(e) => if let Error::Other(_) = e {
                    let mut resp = Response::new();
                    let mut sel_resp = SelectResponse::new();
                    sel_resp.set_error(to_pb_error(&e));
                    resp.set_data(box_try!(sel_resp.write_to_bytes()));
                    resp.set_other_error(format!("{}", e));
                    return Ok((resp, false));
                } else {
                    return Err(e);
                },
            }
        }
    }

    pub fn collect_statistics_into(&mut self, statistics: &mut Statistics) {
        self.exec.collect_statistics_into(statistics);
    }
}

fn response_from_chunks(chunks: Vec<Chunk>, remain: bool) -> Result<(Response, bool)> {
    let mut resp = Response::new();
    let mut sel_resp = SelectResponse::new();
    sel_resp.set
    sel_resp.mut_chunks().push(chunk);
    let data = box_try!(sel_resp.write_to_bytes());
    resp.set_data(data);
    Ok(resp, remain)
}

fn response_from_chunk(chunk: Chunk, remain: bool) -> Result<(Response, bool)> {
    let mut resp = Response::new();
    let mut sel_resp = SelectResponse::new();
    sel_resp.mut_chunks().push(chunk);
    let data = box_try!(sel_resp.write_to_bytes());
    resp.set_data(data);
    Ok(resp, remain)
}

#[inline]
fn inflate_cols(row: &Row, cols: &[ColumnInfo], output_offsets: &[u32]) -> Result<Vec<u8>> {
    let data = &row.data;
    // TODO capacity is not enough
    let mut values = Vec::with_capacity(data.value.len());
    for offset in output_offsets {
        let col = &cols[*offset as usize];
        let col_id = col.get_column_id();
        match data.get(col_id) {
            Some(value) => values.extend_from_slice(value),
            None if col.get_pk_handle() => {
                let pk = get_pk(col, row.handle);
                box_try!(values.encode(&[pk], false));
            }
            None if col.has_default_val() => {
                values.extend_from_slice(col.get_default_val());
            }
            None if mysql::has_not_null_flag(col.get_flag() as u64) => {
                return Err(box_err!("column {} of {} is missing", col_id, row.handle));
            }
            None => {
                box_try!(values.encode(&[Datum::Null], false));
            }
        }
    }
    Ok(values)
}
