// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

//! A module contains test cases for reading on followers.

use std::sync::atomic::*;
use std::sync::{mpsc, Arc};

use raft::eraftpb::MessageType;
use test_raftstore::*;
use tikv::raftstore::store::Callback;
use tikv_util::HandyRwLock;

#[test]
fn test_forward_read() {
    test_util::setup_for_ci();
    let mut cluster = new_node_cluster(0, 3);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();
    cluster.run();

    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    let r1 = cluster.get_region(b"k1");
    let old_leader = cluster.leader_of_region(r1.get_id()).unwrap();
    let new_leader = new_peer(
        (old_leader.get_id() + 1) % 3 + 1,
        (old_leader.get_id() + 1) % 3 + 1,
    );

    // Drop all message append from the new leader.
    let filter = Box::new(
        RegionPacketFilter::new(r1.get_id(), new_leader.get_store_id())
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgAppendResponse)
            .when(Arc::new(AtomicBool::new(true))),
    );
    cluster
        .sim
        .wl()
        .add_recv_filter(new_leader.get_id(), filter);

    cluster.must_transfer_leader(r1.get_id(), new_leader);

    let (tx, rx) = mpsc::sync_channel(1);
    let mut read_request = new_request(
        r1.get_id(),
        r1.get_region_epoch().clone(),
        vec![new_get_cmd(b"k1")],
        false,
    );
    read_request.mut_header().set_peer(old_leader.clone());
    cluster
        .sim
        .wl()
        .async_command_on_node(
            old_leader.get_id(),
            read_request,
            Callback::Read(Box::new(move |resp| tx.send(resp.response).unwrap())),
        )
        .unwrap();
    println!("read resp: {:?}", rx.recv().unwrap());
}
