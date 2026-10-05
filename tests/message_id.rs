//! A keyed send carries its deduplication key in the SRMP envelope's
//! message id, `uuid:<key>@<host>`, the same on every attempt of one
//! Journey; an unkeyed send is numbered by its sender.

use std::thread;

use transport::Transport;
use xmip_core_transport_msmq::MsmqTransport;

/// A Journey's identifier, as the runtime hands it.
const KEY: &str = "0b6f5a52-7c1e-4d0a-9a4e-3f1d2c8b9e70";

#[test]
fn a_keyed_message_carries_the_journey_id_as_its_message_id_on_every_attempt() {
    let far_end = MsmqTransport::loopback();
    let (listener, address) = far_end.bind().expect("bound");
    let taking = thread::spawn(move || {
        (0..3)
            .map(|_| far_end.accept_one(&listener).expect("taken"))
            .collect::<Vec<_>>()
    });
    let near = MsmqTransport::new("127.0.0.1:0", "near");
    let target = format!("msmq://{address}/orders");
    near.send_keyed(&target, b"order", KEY).expect("sent");
    near.send_keyed(&target, b"order", KEY).expect("sent again");
    near.send(&target, b"order").expect("sent unkeyed");
    let taken = taking.join().expect("far end");
    let ids: Vec<&str> = taken
        .iter()
        .map(|taken| taken.origin_uri.split_once('#').expect("an id").1)
        .collect();
    let keyed = format!("uuid:{KEY}@near");
    assert_eq!(ids, [keyed.as_str(), keyed.as_str(), "uuid:3@near"]);
    assert!(taken.iter().all(|taken| taken.bytes == b"order"));
}
