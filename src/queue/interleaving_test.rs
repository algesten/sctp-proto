use super::pending_queue::PendingQueue;
use super::reassembly_queue::ReassemblyQueue;
use crate::chunk::chunk_payload_data::{ChunkPayloadData, PayloadProtocolIdentifier};
use alloc::vec::Vec;
use bytes::Bytes;

fn i_data(mid: u32, fsn: u32, begin: bool, end: bool, payload: &'static [u8]) -> ChunkPayloadData {
    ChunkPayloadData {
        interleaved: true,
        stream_identifier: 1,
        message_identifier: mid,
        fragment_sequence_number: fsn,
        beginning_fragment: begin,
        ending_fragment: end,
        payload_type: PayloadProtocolIdentifier::Binary,
        user_data: Bytes::from_static(payload),
        ..Default::default()
    }
}

#[test]
fn i_data_reassembles_by_mid_and_fsn_not_tsn() {
    let mut queue = ReassemblyQueue::new(1, 1024);
    let mut first = i_data(0, 0, true, false, b"hello ");
    first.tsn = 10;
    let mut second = i_data(1, 0, true, true, b"next");
    second.tsn = 11;
    let mut final_fragment = i_data(0, 1, false, true, b"world");
    final_fragment.tsn = 12;

    assert!(!queue.push(first).unwrap());
    assert!(queue.push(second).unwrap());
    assert!(queue.push(final_fragment).unwrap());

    let first_message = queue.read().unwrap();
    assert_eq!(first_message.len(), 11);
    let second_message = queue.read().unwrap();
    assert_eq!(second_message.len(), 4);
}

#[test]
fn interleaving_scheduler_rotates_complete_message_tails() {
    let mut queue = PendingQueue::new();
    queue.set_interleaving(true);
    for (message_id, fsn) in [(1, 0), (1, 1), (2, 0), (2, 1)] {
        queue.push(ChunkPayloadData {
            message_id,
            fragment_sequence_number: fsn,
            beginning_fragment: fsn == 0,
            ending_fragment: fsn == 1,
            user_data: Bytes::from_static(b"x"),
            ..Default::default()
        });
    }

    let mut observed = Vec::new();
    while let Some(chunk) = queue.peek() {
        let (beginning, unordered) = (chunk.beginning_fragment, chunk.unordered);
        observed.push(queue.pop(beginning, unordered).unwrap().message_id);
    }
    assert_eq!(observed, vec![1, 2, 1, 2]);
}
