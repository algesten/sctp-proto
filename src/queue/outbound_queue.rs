use alloc::collections::VecDeque;
use alloc::vec::Vec;
use std::time::Instant;

use crate::chunk::chunk_payload_data::ChunkPayloadData;
use crate::util::sna32lte;

#[cfg(test)]
#[path = "outbound_queue_test.rs"]
mod tests;

/// Owns a user message until its last fragment is acknowledged or discarded.
#[derive(Debug)]
pub(crate) struct OutboundMessage {
    // The sent prefix stays here until cumulatively acknowledged. Gap ACKs
    // release its payload, but retain the chunk for TSN and FORWARD-TSN lookup.
    chunks: VecDeque<ChunkPayloadData>,
    inflight: usize,
    abandoned: bool,
}

impl OutboundMessage {
    pub(crate) fn new(chunks: Vec<ChunkPayloadData>) -> Self {
        Self {
            chunks: chunks.into(),
            inflight: 0,
            abandoned: false,
        }
    }

    fn pending(&self) -> Option<&ChunkPayloadData> {
        self.chunks.get(self.inflight)
    }
}

/// Sender queue, preserving message ownership across pending and inflight DATA.
///
/// Once a message starts, all its fragments receive consecutive TSNs before
/// another message starts. Only the last started message can have a pending
/// tail, including when its entire sent prefix has already been acknowledged.
#[derive(Debug, Default)]
pub(crate) struct OutboundQueue {
    ordered: VecDeque<OutboundMessage>,
    unordered: VecDeque<OutboundMessage>,
    started: VecDeque<OutboundMessage>,
    pending_len: usize,
    pending_bytes: usize,
    inflight_len: usize,
    inflight_bytes: usize,
}

impl OutboundQueue {
    pub(crate) fn push(&mut self, message: OutboundMessage) {
        let Some(first) = message.chunks.front() else {
            return;
        };
        self.pending_len += message.chunks.len();
        self.pending_bytes += message
            .chunks
            .iter()
            .map(|c| c.user_data.len())
            .sum::<usize>();
        if first.unordered {
            self.unordered.push_back(message);
        } else {
            self.ordered.push_back(message);
        }
    }

    pub(crate) fn peek_pending(&self) -> Option<&ChunkPayloadData> {
        self.started
            .back()
            .and_then(OutboundMessage::pending)
            .or_else(|| self.unordered.front().and_then(OutboundMessage::pending))
            .or_else(|| self.ordered.front().and_then(OutboundMessage::pending))
    }

    /// Assign the next TSN and return a copy for serialization. The queue keeps
    /// ownership of the original fragment and its message throughout flight.
    pub(crate) fn send_next(&mut self, tsn: u32, now: Instant) -> Option<ChunkPayloadData> {
        if self
            .started
            .back()
            .and_then(OutboundMessage::pending)
            .is_none()
        {
            let message = self
                .unordered
                .pop_front()
                .or_else(|| self.ordered.pop_front())?;
            self.started.push_back(message);
        }
        let message = self.started.back_mut()?;
        let chunk = &mut message.chunks[message.inflight];
        chunk.tsn = tsn;
        chunk.since = Some(now);
        chunk.nsent = 1;
        message.inflight += 1;
        self.pending_len -= 1;
        self.pending_bytes -= chunk.user_data.len();
        self.inflight_len += 1;
        self.inflight_bytes += chunk.user_data.len();
        Some(chunk.clone())
    }

    /// Binary search the ordered message ranges, then index the fragment by
    /// TSN offset. Serial arithmetic also covers ranges crossing u32::MAX.
    fn position(&self, tsn: u32) -> Option<(usize, usize)> {
        let index = self
            .started
            .partition_point(|message| {
                message.inflight != 0 && sna32lte(message.chunks[0].tsn, tsn)
            })
            .checked_sub(1)?;
        let message = &self.started[index];
        let offset = tsn.wrapping_sub(message.chunks[0].tsn) as usize;
        (offset < message.inflight).then_some((index, offset))
    }

    pub(crate) fn get(&self, tsn: u32) -> Option<&ChunkPayloadData> {
        let (message, chunk) = self.position(tsn)?;
        Some(&self.started[message].chunks[chunk])
    }

    pub(crate) fn get_mut(&mut self, tsn: u32) -> Option<&mut ChunkPayloadData> {
        let (message, chunk) = self.position(tsn)?;
        Some(&mut self.started[message].chunks[chunk])
    }

    /// Remove only the oldest assigned TSN, as cumulative ACKs advance.
    pub(crate) fn pop(&mut self, tsn: u32) -> Option<ChunkPayloadData> {
        let message = self.started.front_mut()?;
        if message.inflight == 0 || message.chunks.front()?.tsn != tsn {
            return None;
        }
        let chunk = message.chunks.pop_front()?;
        message.inflight -= 1;
        self.inflight_len -= 1;
        self.inflight_bytes -= chunk.user_data.len();
        if message.chunks.is_empty() {
            self.started.pop_front();
        }
        Some(chunk)
    }

    pub(crate) fn mark_as_acked(&mut self, tsn: u32) -> usize {
        let Some(chunk) = self.get_mut(tsn) else {
            return 0;
        };
        chunk.acked = true;
        chunk.retransmit = false;
        let bytes = chunk.user_data.len();
        chunk.user_data.clear();
        self.inflight_bytes -= bytes;
        bytes
    }

    pub(crate) fn is_abandoned(&self, tsn: u32) -> bool {
        self.position(tsn)
            .is_some_and(|(index, _)| self.started[index].abandoned)
    }

    /// Abandon the entire message and discard its unsent tail. Return the
    /// stream and bytes whose buffer credit must be released locally. Sent
    /// chunks retain their TSNs until the peer acknowledges FORWARD-TSN.
    pub(crate) fn abandon(&mut self, tsn: u32) -> Option<(u16, usize)> {
        let (index, _) = self.position(tsn)?;
        let message = &mut self.started[index];
        message.abandoned = true;
        let stream = message.chunks[0].stream_identifier;
        let pending = message.chunks.len() - message.inflight;
        let bytes = message
            .chunks
            .drain(message.inflight..)
            .map(|c| c.user_data.len())
            .sum();
        self.pending_len -= pending;
        self.pending_bytes -= bytes;
        for chunk in &mut message.chunks {
            chunk.retransmit = false;
        }
        Some((stream, bytes))
    }

    pub(crate) fn mark_all_to_retransmit(&mut self) {
        for message in &mut self.started {
            if !message.abandoned {
                for chunk in message.chunks.iter_mut().take(message.inflight) {
                    if !chunk.acked {
                        chunk.retransmit = true;
                    }
                }
            }
        }
    }

    pub(crate) fn pending_contains_stream(&self, stream: u16) -> bool {
        self.started
            .back()
            .into_iter()
            .chain(self.unordered.iter())
            .chain(self.ordered.iter())
            .any(|message| {
                message
                    .pending()
                    .is_some_and(|c| c.stream_identifier == stream)
            })
    }

    pub(crate) fn pending_len(&self) -> usize {
        self.pending_len
    }

    pub(crate) fn pending_bytes(&self) -> usize {
        self.pending_bytes
    }

    pub(crate) fn inflight_len(&self) -> usize {
        self.inflight_len
    }

    pub(crate) fn inflight_bytes(&self) -> usize {
        self.inflight_bytes
    }
}
