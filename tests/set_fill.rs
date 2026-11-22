//! An installed fill is the source of every value sctp-proto draws. This is its own test binary
//! because the fill is process-wide: installing it in the lib tests would feed every other test.

use sctp_proto::{ClientConfig, Endpoint, EndpointConfig, TransportConfig};
use sctp_proto::{generate_snap_token, set_fill};
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

/// Bytes handed out by `counting_fill` so far.
static DRAWN: AtomicUsize = AtomicUsize::new(0);
static NEXT: AtomicU8 = AtomicU8::new(1);

fn counting_fill(buf: &mut [u8]) {
    DRAWN.fetch_add(buf.len(), Ordering::SeqCst);
    for b in buf {
        *b = NEXT.fetch_add(1, Ordering::SeqCst);
    }
}

fn second_fill(_buf: &mut [u8]) {
    panic!("a second set_fill must not replace the first");
}

fn drawn() -> usize {
    DRAWN.load(Ordering::SeqCst)
}

#[test]
fn the_installed_fill_is_the_only_source() {
    assert!(set_fill(counting_fill), "the first set_fill installs");
    assert!(!set_fill(second_fill), "a later set_fill changes nothing");

    // A SNAP token's initiate tag and initial TSN.
    let before = drawn();
    generate_snap_token(&TransportConfig::default()).expect("snap token");
    assert_eq!(
        drawn() - before,
        8,
        "initiate tag and initial TSN come from the fill"
    );

    // An association's id (its verification tag) and initial TSN.
    let mut endpoint = Endpoint::new(Arc::new(EndpointConfig::new()), None);
    let before = drawn();
    endpoint
        .connect(
            std::time::Instant::now(),
            ClientConfig::new(),
            "127.0.0.1:5000".parse().unwrap(),
        )
        .expect("connect");
    assert!(
        drawn() - before >= 8,
        "association id and initial TSN come from the fill"
    );
}
