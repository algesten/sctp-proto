use alloc::vec::Vec;
use std::time::Instant;

use super::payload_queue::PayloadQueue;
use super::pending_queue::PendingQueue;
use crate::chunk::chunk_payload_data::ChunkPayloadData;

#[cfg(test)]
#[path = "outbound_queue_test.rs"]
mod tests;

/// Coordinates the existing flat pending queue and TSN-indexed flight queue.
/// Message tags identify siblings without retaining a per-message allocation
/// or requiring their TSNs to be consecutive. Only abandonment scans siblings.
#[derive(Debug, Default)]
pub(crate) struct OutboundQueue {
    pending: PendingQueue,
    inflight: PayloadQueue,
    next_message_id: u64,
}

impl OutboundQueue {
    pub(crate) fn push(&mut self, chunks: Vec<ChunkPayloadData>) {
        if chunks.is_empty() {
            return;
        }
        let message_id = self.next_message_id;
        // Never alias a still-queued message, even on counter exhaustion.
        self.next_message_id = message_id.checked_add(1).expect("message ID exhausted");
        for mut chunk in chunks {
            chunk.message_id = message_id;
            self.pending.push(chunk);
        }
    }

    pub(crate) fn peek_pending(&self) -> Option<&ChunkPayloadData> {
        self.pending.peek()
    }

    /// Assign the next TSN and retain the chunk in the hash-indexed flight queue.
    pub(crate) fn send_next(
        &mut self,
        next_tsn: &mut u32,
        now: Instant,
    ) -> Option<ChunkPayloadData> {
        let c = self.pending.peek()?;
        let mut chunk = self.pending.pop(c.beginning_fragment, c.unordered)?;
        chunk.tsn = *next_tsn;
        *next_tsn = next_tsn.wrapping_add(1);
        chunk.since = Some(now);
        chunk.nsent = 1;
        self.inflight.push_no_check(chunk.clone());
        Some(chunk)
    }

    pub(crate) fn get(&self, tsn: u32) -> Option<&ChunkPayloadData> {
        self.inflight.get(tsn)
    }

    pub(crate) fn get_mut(&mut self, tsn: u32) -> Option<&mut ChunkPayloadData> {
        self.inflight.get_mut(tsn)
    }

    pub(crate) fn pop(&mut self, tsn: u32) -> Option<ChunkPayloadData> {
        self.inflight.pop(tsn)
    }

    pub(crate) fn mark_as_acked(&mut self, tsn: u32) -> usize {
        self.inflight.mark_as_acked(tsn)
    }

    pub(crate) fn is_abandoned(&self, tsn: u32) -> bool {
        self.inflight.get(tsn).is_some_and(|c| c.abandoned)
    }

    /// Mark every sibling, irrespective of its TSN, and release pending payload.
    /// Reserve a terminal TSN for the unsent tail so FORWARD-TSN advances even if every
    /// transmitted fragment arrived but its SACK was lost. Return buffer credit
    /// to release; TSN reservation and cursor advancement happen together here.
    pub(crate) fn abandon(&mut self, tsn: u32, next_tsn: &mut u32) -> Option<(u16, usize)> {
        let chunk = self.inflight.get(tsn)?;
        let stream = chunk.stream_identifier;
        let message_id = chunk.message_id;
        if chunk.abandoned {
            return Some((stream, 0));
        }
        self.inflight.abandon_message(message_id);
        let mut terminal = None;
        let bytes = self.pending.remove_message(message_id, |mut chunk| {
            chunk.user_data.clear();
            terminal = Some(chunk);
        });
        if let Some(mut chunk) = terminal {
            // These bytes have never been assigned TSNs or transmitted. One
            // empty terminal marker suffices to skip the whole tail, avoiding
            // an inflight map entry for every discarded fragment.
            chunk.tsn = *next_tsn;
            *next_tsn = next_tsn.wrapping_add(1);
            chunk.beginning_fragment = false;
            chunk.ending_fragment = true;
            chunk.abandoned = true;
            chunk.retransmit = false;
            self.inflight.push_no_check(chunk);
        }
        Some((stream, bytes))
    }

    pub(crate) fn mark_all_to_retransmit(&mut self) {
        self.inflight.mark_all_to_retransmit();
    }

    pub(crate) fn pending_contains_stream(&self, stream: u16) -> bool {
        self.pending.contains_stream(stream)
    }

    pub(crate) fn pending_len(&self) -> usize {
        self.pending.len()
    }

    pub(crate) fn pending_bytes(&self) -> usize {
        self.pending.get_num_bytes()
    }

    pub(crate) fn inflight_len(&self) -> usize {
        self.inflight.len()
    }

    pub(crate) fn inflight_bytes(&self) -> usize {
        self.inflight.get_num_bytes()
    }
}
