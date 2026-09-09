use crate::error::{Error, Result};

use bytes::{Bytes, BytesMut};

///////////////////////////////////////////////////////////////////
//payload_queue_test
///////////////////////////////////////////////////////////////////
use super::payload_queue::*;
use crate::chunk::chunk_payload_data::{ChunkPayloadData, PayloadProtocolIdentifier};
use crate::chunk::chunk_selective_ack::GapAckBlock;

fn make_payload(tsn: u32, n_bytes: usize) -> ChunkPayloadData {
    ChunkPayloadData {
        tsn,
        user_data: {
            let mut b = BytesMut::new();
            b.resize(n_bytes, 0);
            b.freeze()
        },
        ..Default::default()
    }
}

#[test]
fn test_payload_queue_push_no_check() -> Result<()> {
    let mut pq = PayloadQueue::new();

    pq.push_no_check(make_payload(0, 10));
    assert_eq!(10, pq.get_num_bytes(), "total bytes mismatch");
    assert_eq!(1, pq.len(), "item count mismatch");
    pq.push_no_check(make_payload(1, 11));
    assert_eq!(21, pq.get_num_bytes(), "total bytes mismatch");
    assert_eq!(2, pq.len(), "item count mismatch");
    pq.push_no_check(make_payload(2, 12));
    assert_eq!(33, pq.get_num_bytes(), "total bytes mismatch");
    assert_eq!(3, pq.len(), "item count mismatch");

    for i in 0..3 {
        assert!(!pq.sorted.is_empty(), "should not be empty");
        let c = pq.pop(i);
        assert!(c.is_some(), "pop should succeed");
        if let Some(c) = c {
            assert_eq!(i, c.tsn, "TSN should match");
        }
    }

    assert_eq!(0, pq.get_num_bytes(), "total bytes mismatch");
    assert_eq!(0, pq.len(), "item count mismatch");

    assert!(pq.sorted.is_empty(), "should be empty");
    pq.push_no_check(make_payload(3, 13));
    assert_eq!(13, pq.get_num_bytes(), "total bytes mismatch");
    pq.push_no_check(make_payload(4, 14));
    assert_eq!(27, pq.get_num_bytes(), "total bytes mismatch");

    for i in 3..5 {
        assert!(!pq.sorted.is_empty(), "should not be empty");
        let c = pq.pop(i);
        assert!(c.is_some(), "pop should succeed");
        if let Some(c) = c {
            assert_eq!(i, c.tsn, "TSN should match");
        }
    }

    assert_eq!(0, pq.get_num_bytes(), "total bytes mismatch");
    assert_eq!(0, pq.len(), "item count mismatch");

    Ok(())
}

#[test]
fn test_payload_queue_get_gap_ack_block() -> Result<()> {
    let mut pq = PayloadQueue::new();

    pq.push(make_payload(1, 0), 0);
    pq.push(make_payload(2, 0), 0);
    pq.push(make_payload(3, 0), 0);
    pq.push(make_payload(4, 0), 0);
    pq.push(make_payload(5, 0), 0);
    pq.push(make_payload(6, 0), 0);

    let gab1 = [GapAckBlock { start: 1, end: 6 }];
    let gab2 = pq.get_gap_ack_blocks(0);
    assert!(!gab2.is_empty());
    assert_eq!(gab2.len(), 1);

    assert_eq!(gab1[0].start, gab2[0].start);
    assert_eq!(gab1[0].end, gab2[0].end);

    pq.push(make_payload(8, 0), 0);
    pq.push(make_payload(9, 0), 0);

    let gab1 = [
        GapAckBlock { start: 1, end: 6 },
        GapAckBlock { start: 8, end: 9 },
    ];
    let gab2 = pq.get_gap_ack_blocks(0);
    assert!(!gab2.is_empty());
    assert_eq!(gab2.len(), 2);

    assert_eq!(gab1[0].start, gab2[0].start);
    assert_eq!(gab1[0].end, gab2[0].end);
    assert_eq!(gab1[1].start, gab2[1].start);
    assert_eq!(gab1[1].end, gab2[1].end);

    Ok(())
}

#[test]
fn test_payload_queue_get_last_tsn_received() -> Result<()> {
    let mut pq = PayloadQueue::new();

    // empty queie should return false
    let ok = pq.get_last_tsn_received();
    assert!(ok.is_none(), "should be none");

    let ok = pq.push(make_payload(20, 0), 0);
    assert!(ok, "should be true");
    let tsn = pq.get_last_tsn_received();
    assert!(tsn.is_some(), "should be false");
    assert_eq!(Some(&20), tsn, "should match");

    // append should work
    let ok = pq.push(make_payload(21, 0), 0);
    assert!(ok, "should be true");
    let tsn = pq.get_last_tsn_received();
    assert!(tsn.is_some(), "should be false");
    assert_eq!(Some(&21), tsn, "should match");

    // check if sorting applied
    let ok = pq.push(make_payload(19, 0), 0);
    assert!(ok, "should be true");
    let tsn = pq.get_last_tsn_received();
    assert!(tsn.is_some(), "should be false");
    assert_eq!(Some(&21), tsn, "should match");

    Ok(())
}

///////////////////////////////////////////////////////////////////
//reassembly_queue_test
///////////////////////////////////////////////////////////////////
use super::reassembly_queue::*;

#[test]
fn test_reassembly_queue_ordered_fragments() -> Result<()> {
    let mut rq = ReassemblyQueue::new(0, 65536);

    let org_ppi = PayloadProtocolIdentifier::Binary;

    let chunk = ChunkPayloadData {
        payload_type: org_ppi,
        beginning_fragment: true,
        tsn: 1,
        stream_sequence_number: 0,
        user_data: Bytes::from_static(b"ABC"),
        ..Default::default()
    };

    let complete = rq.push(chunk).expect("chunk to be queued");
    assert!(!complete, "chunk set should not be complete yet");
    assert_eq!(3, rq.get_num_bytes(), "num bytes mismatch");

    let chunk = ChunkPayloadData {
        payload_type: org_ppi,
        ending_fragment: true,
        tsn: 2,
        stream_sequence_number: 0,
        user_data: Bytes::from_static(b"DEFG"),
        ..Default::default()
    };

    let complete = rq.push(chunk).expect("chunk to be queued");
    assert!(complete, "chunk set should be complete");
    assert_eq!(7, rq.get_num_bytes(), "num bytes mismatch");

    let mut buf = vec![0u8; 16];

    if let Some(chunks) = rq.read() {
        let n = chunks.read(&mut buf)?;
        assert_eq!(7, n, "should received 7 bytes");
        assert_eq!(0, rq.get_num_bytes(), "num bytes mismatch");
        assert_eq!(chunks.ppi, org_ppi, "should have valid ppi");
        assert_eq!(&buf[..n], b"ABCDEFG", "data should match");
    } else {
        panic!();
    }

    Ok(())
}

#[test]
fn test_reassembly_queue_unordered_fragments() -> Result<()> {
    let mut rq = ReassemblyQueue::new(0, 65536);

    let org_ppi = PayloadProtocolIdentifier::Binary;

    let chunk = ChunkPayloadData {
        payload_type: org_ppi,
        unordered: true,
        beginning_fragment: true,
        tsn: 1,
        stream_sequence_number: 0,
        user_data: Bytes::from_static(b"ABC"),
        ..Default::default()
    };

    let complete = rq.push(chunk).expect("chunk to be queued");
    assert!(!complete, "chunk set should not be complete yet");
    assert_eq!(3, rq.get_num_bytes(), "num bytes mismatch");

    let chunk = ChunkPayloadData {
        payload_type: org_ppi,
        unordered: true,
        tsn: 2,
        stream_sequence_number: 0,
        user_data: Bytes::from_static(b"DEFG"),
        ..Default::default()
    };

    let complete = rq.push(chunk).expect("chunk to be queued");
    assert!(!complete, "chunk set should not be complete yet");
    assert_eq!(7, rq.get_num_bytes(), "num bytes mismatch");

    let chunk = ChunkPayloadData {
        payload_type: org_ppi,
        unordered: true,
        ending_fragment: true,
        tsn: 3,
        stream_sequence_number: 0,
        user_data: Bytes::from_static(b"H"),
        ..Default::default()
    };

    let complete = rq.push(chunk).expect("chunk to be queued");
    assert!(complete, "chunk set should be complete");
    assert_eq!(8, rq.get_num_bytes(), "num bytes mismatch");

    let mut buf = vec![0u8; 16];

    if let Some(chunks) = rq.read() {
        let n = chunks.read(&mut buf)?;
        assert_eq!(8, n, "should received 8 bytes");
        assert_eq!(0, rq.get_num_bytes(), "num bytes mismatch");
        assert_eq!(chunks.ppi, org_ppi, "should have valid ppi");
        assert_eq!(&buf[..n], b"ABCDEFGH", "data should match");
    } else {
        panic!();
    }

    Ok(())
}

#[test]
fn test_reassembly_queue_ordered_and_unordered_fragments() -> Result<()> {
    let mut rq = ReassemblyQueue::new(0, 65536);
    let org_ppi = PayloadProtocolIdentifier::Binary;
    let chunk = ChunkPayloadData {
        payload_type: org_ppi,
        beginning_fragment: true,
        ending_fragment: true,
        tsn: 1,
        stream_sequence_number: 0,
        user_data: Bytes::from_static(b"ABC"),
        ..Default::default()
    };

    let complete = rq.push(chunk).expect("chunk to be queued");
    assert!(complete, "chunk set should be complete");
    assert_eq!(3, rq.get_num_bytes(), "num bytes mismatch");

    let chunk = ChunkPayloadData {
        payload_type: org_ppi,
        unordered: true,
        beginning_fragment: true,
        ending_fragment: true,
        tsn: 2,
        stream_sequence_number: 1,
        user_data: Bytes::from_static(b"DEF"),
        ..Default::default()
    };

    let complete = rq.push(chunk).expect("chunk to be queued");
    assert!(complete, "chunk set should be complete");
    assert_eq!(6, rq.get_num_bytes(), "num bytes mismatch");

    //
    // Now we have two complete chunks ready to read in the reassemblyQueue.
    //

    let mut buf = vec![0u8; 16];

    // Should read unordered chunks first
    if let Some(chunks) = rq.read() {
        let n = chunks.read(&mut buf)?;
        assert_eq!(3, n, "should received 3 bytes");
        assert_eq!(3, rq.get_num_bytes(), "num bytes mismatch");
        assert_eq!(chunks.ppi, org_ppi, "should have valid ppi");
        assert_eq!(&buf[..n], b"DEF", "data should match");
    } else {
        panic!();
    }

    // Next should read ordered chunks
    if let Some(chunks) = rq.read() {
        let n = chunks.read(&mut buf)?;
        assert_eq!(3, n, "should received 3 bytes");
        assert_eq!(0, rq.get_num_bytes(), "num bytes mismatch");
        assert_eq!(chunks.ppi, org_ppi, "should have valid ppi");
        assert_eq!(&buf[..n], b"ABC", "data should match");
    } else {
        panic!();
    }

    Ok(())
}

#[test]
fn test_reassembly_queue_unordered_complete_skips_incomplete() -> Result<()> {
    let mut rq = ReassemblyQueue::new(0, 65536);

    let org_ppi = PayloadProtocolIdentifier::Binary;

    let chunk = ChunkPayloadData {
        payload_type: org_ppi,
        unordered: true,
        beginning_fragment: true,
        tsn: 10,
        stream_sequence_number: 0,
        user_data: Bytes::from_static(b"IN"),
        ..Default::default()
    };

    let complete = rq.push(chunk).expect("chunk to be queued");
    assert!(!complete, "chunk set should not be complete yet");
    assert_eq!(2, rq.get_num_bytes(), "num bytes mismatch");

    let chunk = ChunkPayloadData {
        payload_type: org_ppi,
        unordered: true,
        ending_fragment: true,
        tsn: 12, // <- incongiguous
        stream_sequence_number: 1,
        user_data: Bytes::from_static(b"COMPLETE"),
        ..Default::default()
    };

    let complete = rq.push(chunk).expect("chunk to be queued");
    assert!(!complete, "chunk set should not be complete yet");
    assert_eq!(10, rq.get_num_bytes(), "num bytes mismatch");

    let chunk = ChunkPayloadData {
        payload_type: org_ppi,
        unordered: true,
        beginning_fragment: true,
        ending_fragment: true,
        tsn: 13,
        stream_sequence_number: 1,
        user_data: Bytes::from_static(b"GOOD"),
        ..Default::default()
    };

    let complete = rq.push(chunk).expect("chunk to be queued");
    assert!(complete, "chunk set should be complete");
    assert_eq!(14, rq.get_num_bytes(), "num bytes mismatch");

    //
    // Now we have two complete chunks ready to read in the reassemblyQueue.
    //

    let mut buf = vec![0u8; 16];

    // Should pick the one that has "GOOD"
    if let Some(chunks) = rq.read() {
        let n = chunks.read(&mut buf)?;
        assert_eq!(4, n, "should receive 4 bytes");
        assert_eq!(10, rq.get_num_bytes(), "num bytes mismatch");
        assert_eq!(chunks.ppi, org_ppi, "should have valid ppi");
        assert_eq!(&buf[..n], b"GOOD", "data should match");
    } else {
        panic!();
    }

    Ok(())
}

#[test]
fn test_reassembly_queue_ignores_chunk_with_wrong_si() -> Result<()> {
    let mut rq = ReassemblyQueue::new(123, 65536);

    let org_ppi = PayloadProtocolIdentifier::Binary;

    let chunk = ChunkPayloadData {
        payload_type: org_ppi,
        stream_identifier: 124,
        beginning_fragment: true,
        ending_fragment: true,
        tsn: 10,
        stream_sequence_number: 0,
        user_data: Bytes::from_static(b"IN"),
        ..Default::default()
    };

    let complete = rq.push(chunk).expect("chunk to be queued");
    assert!(!complete, "chunk should be ignored");
    assert_eq!(0, rq.get_num_bytes(), "num bytes mismatch");
    Ok(())
}

#[test]
fn test_reassembly_queue_ignores_chunk_with_stale_ssn() -> Result<()> {
    let mut rq = ReassemblyQueue::new(0, 65536);
    rq.next_ssn = 7; // forcibly set expected SSN to 7

    let org_ppi = PayloadProtocolIdentifier::Binary;

    let chunk = ChunkPayloadData {
        payload_type: org_ppi,
        beginning_fragment: true,
        ending_fragment: true,
        tsn: 10,
        stream_sequence_number: 6, // <-- stale
        user_data: Bytes::from_static(b"IN"),
        ..Default::default()
    };

    let complete = rq.push(chunk).expect("chunk to be queued");
    assert!(!complete, "chunk should not be ignored");
    assert_eq!(0, rq.get_num_bytes(), "num bytes mismatch");

    Ok(())
}

#[test]
fn test_reassembly_queue_should_fail_to_read_incomplete_chunk() -> Result<()> {
    let mut rq = ReassemblyQueue::new(0, 65536);

    let org_ppi = PayloadProtocolIdentifier::Binary;

    let chunk = ChunkPayloadData {
        payload_type: org_ppi,
        beginning_fragment: true,
        tsn: 123,
        stream_sequence_number: 0,
        user_data: Bytes::from_static(b"IN"),
        ..Default::default()
    };

    let complete = rq.push(chunk).expect("chunk to be queued");
    assert!(!complete, "the set should not be complete");
    assert_eq!(2, rq.get_num_bytes(), "num bytes mismatch");

    let result = rq.read();
    assert!(result.is_none(), "read() should not succeed");
    assert_eq!(2, rq.get_num_bytes(), "num bytes mismatch");

    Ok(())
}

#[test]
fn test_reassembly_queue_should_fail_to_read_if_the_nex_ssn_is_not_ready() -> Result<()> {
    let mut rq = ReassemblyQueue::new(0, 65536);

    let org_ppi = PayloadProtocolIdentifier::Binary;

    let chunk = ChunkPayloadData {
        payload_type: org_ppi,
        beginning_fragment: true,
        ending_fragment: true,
        tsn: 123,
        stream_sequence_number: 1,
        user_data: Bytes::from_static(b"IN"),
        ..Default::default()
    };

    let complete = rq.push(chunk).expect("chunk to be queued");
    assert!(complete, "the set should be complete");
    assert_eq!(2, rq.get_num_bytes(), "num bytes mismatch");

    let result = rq.read();
    assert!(result.is_none(), "read() should not succeed");
    assert_eq!(2, rq.get_num_bytes(), "num bytes mismatch");

    Ok(())
}

#[test]
fn test_reassembly_queue_detect_buffer_too_short() -> Result<()> {
    let mut rq = ReassemblyQueue::new(0, 65536);

    let org_ppi = PayloadProtocolIdentifier::Binary;

    let chunk = ChunkPayloadData {
        payload_type: org_ppi,
        beginning_fragment: true,
        ending_fragment: true,
        tsn: 123,
        stream_sequence_number: 0,
        user_data: Bytes::from_static(b"0123456789"),
        ..Default::default()
    };

    let complete = rq.push(chunk).expect("chunk to be queued");
    assert!(complete, "the set should be complete");
    assert_eq!(10, rq.get_num_bytes(), "num bytes mismatch");

    let mut buf = vec![0u8; 8]; // <- passing buffer too short
    if let Some(chunks) = rq.read() {
        let result = chunks.read(&mut buf);
        assert!(result.is_err(), "read() should not succeed");
        if let Err(err) = result {
            assert_eq!(Error::ErrShortBuffer, err, "read() should not succeed");
        }
        assert_eq!(0, rq.get_num_bytes(), "num bytes mismatch");
    } else {
        panic!();
    }

    Ok(())
}

#[test]
fn test_reassembly_queue_forward_tsn_for_ordered_framents() -> Result<()> {
    let mut rq = ReassemblyQueue::new(0, 65536);

    let org_ppi = PayloadProtocolIdentifier::Binary;

    let ssn_complete = 5u16;
    let ssn_dropped = 6u16;

    let chunk = ChunkPayloadData {
        payload_type: org_ppi,
        beginning_fragment: true,
        ending_fragment: true,
        tsn: 10,
        stream_sequence_number: ssn_complete,
        user_data: Bytes::from_static(b"123"),
        ..Default::default()
    };

    let complete = rq.push(chunk).expect("chunk to be queued");
    assert!(complete, "chunk set should be complete");
    assert_eq!(3, rq.get_num_bytes(), "num bytes mismatch");

    let chunk = ChunkPayloadData {
        payload_type: org_ppi,
        beginning_fragment: true,
        tsn: 11,
        stream_sequence_number: ssn_dropped,
        user_data: Bytes::from_static(b"ABC"),
        ..Default::default()
    };

    let complete = rq.push(chunk).expect("chunk to be queued");
    assert!(!complete, "chunk set should not be complete yet");
    assert_eq!(6, rq.get_num_bytes(), "num bytes mismatch");

    let chunk = ChunkPayloadData {
        payload_type: org_ppi,
        tsn: 12,
        stream_sequence_number: ssn_dropped,
        user_data: Bytes::from_static(b"DEF"),
        ..Default::default()
    };

    let complete = rq.push(chunk).expect("chunk to be queued");
    assert!(!complete, "chunk set should not be complete yet");
    assert_eq!(9, rq.get_num_bytes(), "num bytes mismatch");

    rq.forward_tsn_for_ordered(ssn_dropped);

    assert_eq!(1, rq.ordered.len(), "there should be one chunk left");
    assert_eq!(3, rq.get_num_bytes(), "num bytes mismatch");

    Ok(())
}

#[test]
fn test_reassembly_queue_forward_tsn_for_unordered_framents() -> Result<()> {
    let mut rq = ReassemblyQueue::new(0, 65536);

    let org_ppi = PayloadProtocolIdentifier::Binary;

    let ssn_dropped = 6u16;
    let ssn_kept = 7u16;

    let chunk = ChunkPayloadData {
        payload_type: org_ppi,
        unordered: true,
        beginning_fragment: true,
        tsn: 11,
        stream_sequence_number: ssn_dropped,
        user_data: Bytes::from_static(b"ABC"),
        ..Default::default()
    };

    let complete = rq.push(chunk).expect("chunk to be queued");
    assert!(!complete, "chunk set should not be complete yet");
    assert_eq!(3, rq.get_num_bytes(), "num bytes mismatch");

    let chunk = ChunkPayloadData {
        payload_type: org_ppi,
        unordered: true,
        tsn: 12,
        stream_sequence_number: ssn_dropped,
        user_data: Bytes::from_static(b"DEF"),
        ..Default::default()
    };

    let complete = rq.push(chunk).expect("chunk to be queued");
    assert!(!complete, "chunk set should not be complete yet");
    assert_eq!(6, rq.get_num_bytes(), "num bytes mismatch");

    let chunk = ChunkPayloadData {
        payload_type: org_ppi,
        unordered: true,
        tsn: 14,
        beginning_fragment: true,
        stream_sequence_number: ssn_kept,
        user_data: Bytes::from_static(b"SOS"),
        ..Default::default()
    };

    let complete = rq.push(chunk).expect("chunk to be queued");
    assert!(!complete, "chunk set should not be complete yet");
    assert_eq!(9, rq.get_num_bytes(), "num bytes mismatch");

    // At this point, there are 3 chunks in the rq.unorderedChunks.
    // This call should remove chunks with tsn equals to 13 or older.
    rq.forward_tsn_for_unordered(13);

    // As a result, there should be one chunk (tsn=14)
    assert_eq!(
        1,
        rq.unordered_chunks.len(),
        "there should be one chunk kept"
    );
    assert_eq!(3, rq.get_num_bytes(), "num bytes mismatch");

    Ok(())
}

#[test]
fn test_chunk_set_empty_chunk_set() -> Result<()> {
    let cset = Chunks::new(0, PayloadProtocolIdentifier::default(), vec![]);
    assert!(!cset.is_complete(), "empty chunkSet cannot be complete");
    Ok(())
}

#[test]
fn test_chunk_set_push_dup_chunks_to_chunk_set() -> Result<()> {
    let mut cset = Chunks::new(0, PayloadProtocolIdentifier::default(), vec![]);
    cset.push(ChunkPayloadData {
        tsn: 100,
        beginning_fragment: true,
        ..Default::default()
    });
    let complete = cset.push(ChunkPayloadData {
        tsn: 100,
        ending_fragment: true,
        ..Default::default()
    });
    assert!(!complete, "chunk with dup TSN is not complete");
    assert_eq!(1, cset.chunks.len(), "chunk with dup TSN should be ignored");
    Ok(())
}

#[test]
fn test_chunk_set_incomplete_chunk_set_no_beginning() -> Result<()> {
    let cset = Chunks::new(0, PayloadProtocolIdentifier::default(), vec![]);
    assert!(
        !cset.is_complete(),
        "chunkSet not starting with B=1 cannot be complete"
    );
    Ok(())
}

#[test]
fn test_chunk_set_incomplete_chunk_set_no_contiguous_tsn() -> Result<()> {
    let cset = Chunks::new(
        0,
        PayloadProtocolIdentifier::default(),
        vec![
            ChunkPayloadData {
                tsn: 100,
                beginning_fragment: true,
                ..Default::default()
            },
            ChunkPayloadData {
                tsn: 101,
                ..Default::default()
            },
            ChunkPayloadData {
                tsn: 103,
                ending_fragment: true,
                ..Default::default()
            },
        ],
    );
    assert!(
        !cset.is_complete(),
        "chunkSet not starting with incontiguous tsn cannot be complete"
    );
    Ok(())
}

#[test]
fn test_reassembly_queue_ssn_overflow() -> Result<()> {
    let mut rq = ReassemblyQueue::new(0, 65536);
    let org_ppi = PayloadProtocolIdentifier::Binary;

    for stream_sequence_number in 0..=u16::MAX {
        let chunk = ChunkPayloadData {
            payload_type: org_ppi,
            beginning_fragment: true,
            ending_fragment: true,
            tsn: 10,
            stream_sequence_number,
            user_data: Bytes::from_static(b"123"),
            ..Default::default()
        };
        assert!(rq.push(chunk).expect("chunk to be queued"));
        assert!(rq.read().is_some());
    }

    Ok(())
}

#[test]
fn test_reassembly_queue_ssn_overflow_in_forward_tsn_for_ordered() -> Result<()> {
    let mut rq = ReassemblyQueue::new(0, 65536);
    let org_ppi = PayloadProtocolIdentifier::Binary;

    for stream_sequence_number in 0..u16::MAX {
        let chunk = ChunkPayloadData {
            payload_type: org_ppi,
            beginning_fragment: true,
            ending_fragment: true,
            tsn: 10,
            stream_sequence_number,
            user_data: Bytes::from_static(b"123"),
            ..Default::default()
        };
        assert!(rq.push(chunk).expect("chunk to be queued"));
        assert!(rq.read().is_some());
    }
    rq.forward_tsn_for_ordered(u16::MAX);

    Ok(())
}

#[test]
fn test_chunk_set_wrap() {
    let cset = Chunks::new(
        0,
        PayloadProtocolIdentifier::default(),
        vec![
            ChunkPayloadData {
                tsn: u32::MAX - 1,
                beginning_fragment: true,
                ..Default::default()
            },
            ChunkPayloadData {
                tsn: u32::MAX,
                ..Default::default()
            },
            ChunkPayloadData {
                tsn: 0,
                ending_fragment: true,
                ..Default::default()
            },
        ],
    );

    assert!(
        cset.is_complete(),
        "chunkSet with wrapping TSNs is not complete"
    );
}

#[test]
fn test_reassembly_queue_wrap_find_complete() {
    let mut rq = ReassemblyQueue::new(0, 65535);

    assert!(
        rq.push(ChunkPayloadData {
            tsn: u32::MAX - 1,
            unordered: true,
            beginning_fragment: true,
            ..Default::default()
        })
        .is_ok()
    );
    assert!(
        rq.push(ChunkPayloadData {
            tsn: u32::MAX,
            unordered: true,
            ..Default::default()
        })
        .is_ok()
    );
    assert!(
        rq.push(ChunkPayloadData {
            tsn: 0,
            unordered: true,
            ending_fragment: true,
            ..Default::default()
        })
        .is_ok()
    );

    assert_eq!(
        rq.unordered.len(),
        1,
        "chunkSet with wrapping TSNs is not complete"
    );
}

#[test]
fn test_reassembly_queue_max_message_size() {
    let mut rq = ReassemblyQueue::new(0, 65536);
    rq.max_message_size = 15;

    assert_eq!(
        rq.push(ChunkPayloadData {
            tsn: 100,
            beginning_fragment: true,
            unordered: true,
            user_data: Bytes::from_owner([0; 5]),
            ..Default::default()
        }),
        Ok(false)
    );
    assert_eq!(
        rq.push(ChunkPayloadData {
            tsn: 102,
            ending_fragment: true,
            unordered: true,
            user_data: Bytes::from_owner([0; 5]),
            ..Default::default()
        }),
        Ok(false)
    );
    assert_eq!(
        rq.push(ChunkPayloadData {
            tsn: 103,
            beginning_fragment: true,
            unordered: true,
            user_data: Bytes::from_owner([0; 5]),
            ..Default::default()
        }),
        Ok(false)
    );
    assert_eq!(
        rq.push(ChunkPayloadData {
            tsn: 104,
            unordered: true,
            user_data: Bytes::from_owner([0; 5]),
            ..Default::default()
        }),
        Ok(false)
    );
    assert_eq!(
        rq.push(ChunkPayloadData {
            tsn: 101,
            unordered: true,
            user_data: Bytes::from_owner([0; 5]),
            ..Default::default()
        }),
        Ok(true)
    );
}
