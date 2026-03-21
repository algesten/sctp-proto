#![no_main]

//! Fuzz target for SCTP packet unmarshalling.
//!
//! Feeds arbitrary bytes with a valid CRC32C checksum into
//! `Packet::unmarshal` to find panics or other issues in
//! the parsing logic.

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    sctp_proto::_fuzz::fuzz_packet_unmarshal(data);
});
