use super::*;
use bytes::Bytes;

fn message(stream: u16, unordered: bool, fragments: usize) -> OutboundMessage {
    OutboundMessage::new(
        (0..fragments)
            .map(|i| ChunkPayloadData {
                stream_identifier: stream,
                unordered,
                beginning_fragment: i == 0,
                ending_fragment: i + 1 == fragments,
                user_data: Bytes::from_static(b"fragment"),
                ..Default::default()
            })
            .collect(),
    )
}

#[test]
fn unordered_priority_and_message_selection_survive_prefix_ack() {
    let now = Instant::now();
    let mut q = OutboundQueue::default();
    q.push(message(1, false, 3));
    q.push(message(2, false, 1));
    q.push(message(3, true, 1));
    q.push(message(4, true, 1));
    assert_eq!(q.pending_len(), 6);
    assert_eq!(q.pending_bytes(), 48);

    for (tsn, stream) in [(10, 3), (11, 4), (12, 1)] {
        assert_eq!(q.peek_pending().unwrap().stream_identifier, stream);
        assert_eq!(q.send_next(tsn, now).unwrap().stream_identifier, stream);
        q.pop(tsn).unwrap();
    }
    // The entire sent prefix is gone, but the selected message must survive.
    assert_eq!(q.inflight_len(), 0);
    assert_eq!(q.inflight_bytes(), 0);
    assert_eq!(q.started.len(), 1);
    assert!(q.pending_contains_stream(1));
    assert!(q.pending_contains_stream(2));
    assert!(!q.pending_contains_stream(3));
    q.push(message(5, true, 1));
    for (tsn, stream) in [(13, 1), (14, 1), (15, 5), (16, 2)] {
        let c = q.send_next(tsn, now).unwrap();
        assert_eq!(c.stream_identifier, stream);
        assert_eq!(q.get(tsn).unwrap().tsn, tsn);
        assert!(q.get(tsn.wrapping_sub(1)).is_none());
        q.pop(tsn).unwrap();
    }
    assert!(q.peek_pending().is_none());
    assert!(q.send_next(17, now).is_none());
    assert_eq!(q.pending_bytes(), 0);
    assert_eq!(q.pending_len(), 0);
    assert_eq!(q.inflight_bytes(), 0);
    assert!(q.started.is_empty());
}

#[test]
fn abandonment_owns_pending_tail_and_does_not_alias_unordered_messages() {
    let now = Instant::now();
    let mut q = OutboundQueue::default();
    // Both messages deliberately have the same stream and SSN.
    q.push(message(7, true, 5));
    q.push(message(7, true, 1));
    for tsn in 1..=3 {
        q.send_next(tsn, now).unwrap();
    }
    q.pop(1).unwrap();
    assert_eq!(q.mark_as_acked(3), 8);
    assert_eq!(q.mark_as_acked(3), 0);
    assert_eq!(q.inflight_bytes(), 8);
    assert_eq!(q.abandon(2), Some((7, 16, 2)));
    assert!(q.is_abandoned(2));
    assert!(q.is_abandoned(3));
    assert!(!q.is_abandoned(1));
    assert_eq!(q.pending_len(), 1);
    assert_eq!(q.pending_bytes(), 8);
    assert_eq!(q.abandon(3), Some((7, 0, 0)), "release each byte only once");

    // The unsent tail has TSNs but no payload or retransmissions. The next
    // message must follow those reserved TSNs.
    for tsn in 4..=5 {
        assert!(q.is_abandoned(tsn));
        assert!(q.get(tsn).unwrap().user_data.is_empty());
        assert_eq!(q.get(tsn).unwrap().nsent, 0);
        assert!(!q.get(tsn).unwrap().retransmit);
    }
    let next = q.send_next(6, now).unwrap();
    assert!(next.beginning_fragment && next.ending_fragment);
    assert!(!q.is_abandoned(6));
    assert!(!q.pending_contains_stream(7));
    for tsn in 2..=6 {
        q.pop(tsn).unwrap();
    }
    assert!(q.started.is_empty());
    assert_eq!(q.inflight_bytes(), 0);
    assert_eq!(q.inflight_len(), 0);
}

#[test]
fn retransmission_excludes_pending_acked_and_abandoned_fragments() {
    let now = Instant::now();
    let mut q = OutboundQueue::default();
    q.push(message(1, false, 2));
    q.push(message(2, false, 3));
    for tsn in 1..=4 {
        q.send_next(tsn, now).unwrap();
    }
    q.mark_all_to_retransmit();
    assert_eq!(q.mark_as_acked(2), 8);
    assert_eq!(q.abandon(1), Some((1, 0, 0)));
    q.mark_all_to_retransmit();
    for tsn in 1..=4 {
        assert_eq!(q.get(tsn).unwrap().retransmit, tsn >= 3);
    }
    assert!(!q.peek_pending().unwrap().retransmit);
    assert!(q.get(5).is_none(), "pending chunks have no TSN yet");
    assert_eq!(q.inflight_bytes(), 24);
    assert_eq!(q.pending_bytes(), 8);
    q.get_mut(3).unwrap().nsent += 1;
    assert_eq!(q.get(3).unwrap().nsent, 2);
    assert_eq!(q.get(4).unwrap().nsent, 1);
}

#[test]
fn tsn_lookup_and_retirement_across_wraparound() {
    let now = Instant::now();
    let mut q = OutboundQueue::default();
    let first = u32::MAX - 4;
    for stream in 0..4 {
        q.push(message(stream, false, 3));
    }
    for i in 0..12 {
        q.send_next(first.wrapping_add(i), now).unwrap();
    }
    assert!(q.get(first.wrapping_sub(1)).is_none());
    assert!(q.get(first.wrapping_add(12)).is_none());
    assert!(q.pop(first.wrapping_add(1)).is_none());
    assert_eq!(q.abandon(u32::MAX), Some((1, 0, 0)));
    for i in 0..12 {
        let tsn = first.wrapping_add(i);
        for remaining in i..12 {
            let next = first.wrapping_add(remaining);
            assert_eq!(
                q.get(next).unwrap().stream_identifier,
                (remaining / 3) as u16
            );
            assert_eq!(q.is_abandoned(next), (3..6).contains(&remaining));
        }
        assert_eq!(q.pop(tsn).unwrap().tsn, tsn);
        assert!(q.get(tsn).is_none());
        assert_eq!(q.inflight_len(), (11 - i) as usize);
        assert_eq!(q.inflight_bytes(), (11 - i) as usize * 8);
    }
    assert!(q.started.is_empty());
}

#[test]
fn message_lookup_after_deque_storage_wraps() {
    let now = Instant::now();
    let mut q = OutboundQueue::default();
    // Keep several messages in flight while repeatedly retiring and adding
    // messages, exercising the two physical slices of the message deque.
    for tsn in 0..128u32 {
        q.push(message(tsn as u16, false, 1));
        q.send_next(tsn, now).unwrap();
        if tsn >= 8 {
            q.pop(tsn - 8).unwrap();
        }
        for outstanding in tsn.saturating_sub(7)..=tsn {
            assert_eq!(
                q.get(outstanding).unwrap().stream_identifier,
                outstanding as u16
            );
        }
    }
    for tsn in 120..128 {
        q.pop(tsn).unwrap();
    }
    assert!(q.started.is_empty());
}

#[test]
fn reserving_abandoned_tail_tsns_crosses_wraparound() {
    let now = Instant::now();
    let mut q = OutboundQueue::default();
    q.push(message(1, false, 4));
    q.push(message(2, false, 1));
    q.send_next(u32::MAX - 1, now).unwrap();
    assert_eq!(q.abandon(u32::MAX - 1), Some((1, 24, 3)));
    assert_eq!(q.inflight_len(), 4);
    assert_eq!(q.inflight_bytes(), 8);
    q.mark_all_to_retransmit();
    for tsn in [u32::MAX - 1, u32::MAX, 0, 1] {
        assert!(q.is_abandoned(tsn));
        assert!(!q.get(tsn).unwrap().retransmit);
    }
    q.send_next(2, now).unwrap();
    assert_eq!(q.get(2).unwrap().stream_identifier, 2);
    assert!(!q.is_abandoned(2));
    for tsn in [u32::MAX - 1, u32::MAX, 0, 1, 2] {
        q.pop(tsn).unwrap();
    }
    assert_eq!(q.inflight_bytes(), 0);
    assert!(q.started.is_empty());
}

#[test]
fn empty_message_does_not_block_scheduling() {
    let now = Instant::now();
    let mut q = OutboundQueue::default();
    q.push(message(1, true, 0));
    assert!(q.peek_pending().is_none());
    assert!(q.send_next(1, now).is_none());
    assert!(q.get(1).is_none());
    assert_eq!(q.abandon(1), None);
    assert_eq!(q.mark_as_acked(1), 0);
    q.push(message(2, false, 1));
    assert_eq!(q.send_next(1, now).unwrap().stream_identifier, 2);
    assert_eq!(q.pop(1).unwrap().stream_identifier, 2);
    assert_eq!(q.pending_len(), 0);
    assert_eq!(q.inflight_len(), 0);
}
