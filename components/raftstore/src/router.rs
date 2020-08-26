// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crossbeam::{SendError, TrySendError};
use engine_traits::{KvEngine, Snapshot};
use kvproto::raft_cmdpb::RaftCmdRequest;
use kvproto::raft_serverpb::RaftMessage;
use raft::SnapshotStatus;
use raft_engine::RaftEngine;
use txn_types::TxnExtra;

use crate::store::fsm::RaftRouter;
use crate::store::transport::{CasualRouter, ProposalRouter, StoreRouter};
use crate::store::{Callback, CasualMessage, PeerMsg, RaftCommand, SignificantMsg, StoreMsg};
use crate::{DiscardReason, Error as RaftStoreError, Result as RaftStoreResult};

/// Routes messages to the raftstore.
pub trait RaftStoreRouter<EK>:
    StoreRouter + ProposalRouter<EK::Snapshot> + CasualRouter<EK> + Send + Clone
where
    EK: KvEngine,
{
    /// Sends RaftMessage to local store.
    fn send_raft_msg(&self, msg: RaftMessage) -> RaftStoreResult<()>;

    /// Sends a significant message. We should guarantee that the message can't be dropped.
    fn significant_send(
        &self,
        region_id: u64,
        msg: SignificantMsg<EK::Snapshot>,
    ) -> RaftStoreResult<()>;

    fn send_casual_msg(&self, region_id: u64, msg: CasualMessage<EK>) -> RaftStoreResult<()> {
        <Self as CasualRouter<EK>>::send(self, region_id, msg)
    }

    fn send_store_msg(&self, msg: StoreMsg) -> RaftStoreResult<()> {
        <Self as StoreRouter>::send(self, msg)
    }

    /// Sends RaftCmdRequest to local store.
    fn send_command(&self, req: RaftCmdRequest, cb: Callback<EK::Snapshot>) -> RaftStoreResult<()> {
        self.send_command_txn_extra(req, TxnExtra::default(), cb)
    }

    /// Sends RaftCmdRequest to local store with txn extras.
    fn send_command_txn_extra(
        &self,
        req: RaftCmdRequest,
        txn_extra: TxnExtra,
        cb: Callback<EK::Snapshot>,
    ) -> RaftStoreResult<()> {
        let region_id = req.get_header().get_region_id();
        let cmd = RaftCommand::with_txn_extra(req, cb, txn_extra);
        <Self as ProposalRouter<EK::Snapshot>>::send(self, cmd)
            .map_err(|e| handle_send_error(region_id, e))
    }

    /// Reports the peer being unreachable to the Region.
    fn report_unreachable(&self, region_id: u64, to_peer_id: u64) -> RaftStoreResult<()> {
        let msg = SignificantMsg::Unreachable {
            region_id,
            to_peer_id,
        };
        self.significant_send(region_id, msg)
    }

    /// Reports the sending snapshot status to the peer of the Region.
    fn report_snapshot_status(
        &self,
        region_id: u64,
        to_peer_id: u64,
        status: SnapshotStatus,
    ) -> RaftStoreResult<()> {
        let msg = SignificantMsg::SnapshotStatus {
            region_id,
            to_peer_id,
            status,
        };
        self.significant_send(region_id, msg)
    }

    fn broadcast_unreachable(&self, store_id: u64) {
        let _ = self.send_store_msg(StoreMsg::StoreUnreachable { store_id });
    }
}

#[derive(Clone)]
pub struct RaftStoreBlackHole;

impl<EK: KvEngine> CasualRouter<EK> for RaftStoreBlackHole {
    fn send(&self, _: u64, _: CasualMessage<EK>) -> RaftStoreResult<()> {
        Ok(())
    }
}

impl<S: Snapshot> ProposalRouter<S> for RaftStoreBlackHole {
    fn send(&self, _: RaftCommand<S>) -> std::result::Result<(), TrySendError<RaftCommand<S>>> {
        Ok(())
    }
}

impl StoreRouter for RaftStoreBlackHole {
    fn send(&self, _: StoreMsg) -> RaftStoreResult<()> {
        Ok(())
    }
}

impl<EK> RaftStoreRouter<EK> for RaftStoreBlackHole
where
    EK: KvEngine,
{
    /// Sends RaftMessage to local store.
    fn send_raft_msg(&self, _: RaftMessage) -> RaftStoreResult<()> {
        Ok(())
    }

    /// Sends a significant message. We should guarantee that the message can't be dropped.
    fn significant_send(&self, _: u64, _: SignificantMsg<EK::Snapshot>) -> RaftStoreResult<()> {
        Ok(())
    }
}

#[inline]
pub fn handle_send_error<T>(region_id: u64, e: TrySendError<T>) -> RaftStoreError {
    match e {
        TrySendError::Full(_) => RaftStoreError::Transport(DiscardReason::Full),
        TrySendError::Disconnected(_) => RaftStoreError::RegionNotFound(region_id),
    }
}

impl<EK: KvEngine, ER: RaftEngine> RaftStoreRouter<EK> for RaftRouter<EK, ER> {
    fn send_raft_msg(&self, msg: RaftMessage) -> RaftStoreResult<()> {
        let region_id = msg.get_region_id();
        self.send_raft_message(msg)
            .map_err(|e| handle_send_error(region_id, e))
    }

    fn significant_send(
        &self,
        region_id: u64,
        msg: SignificantMsg<EK::Snapshot>,
    ) -> RaftStoreResult<()> {
        if let Err(SendError(msg)) = self
            .router
            .force_send(region_id, PeerMsg::SignificantMsg(msg))
        {
            // TODO: panic here once we can detect system is shutting down reliably.
            error!("failed to send significant msg"; "msg" => ?msg);
            return Err(RaftStoreError::RegionNotFound(region_id));
        }

        Ok(())
    }
}
