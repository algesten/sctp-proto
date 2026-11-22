//! Without the `rand` feature, a draw with no fill installed panics: nothing is ever drawn from a
//! source the embedder did not choose. CI runs it with `--no-default-features --test no_rand`.

#![cfg(not(feature = "rand"))]

use sctp_proto::{ClientConfig, Endpoint, EndpointConfig, TransportConfig};
use std::sync::Arc;

#[test]
#[should_panic(expected = "no random fill installed")]
fn a_snap_token_with_no_fill_panics() {
    let _ = sctp_proto::generate_snap_token(&TransportConfig::default());
}

#[test]
#[should_panic(expected = "no random fill installed")]
fn an_association_with_no_fill_panics() {
    let mut endpoint = Endpoint::new(Arc::new(EndpointConfig::new()), None);
    let _ = endpoint.connect(
        std::time::Instant::now(),
        ClientConfig::new(),
        "127.0.0.1:5000".parse().unwrap(),
    );
}
