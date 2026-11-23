use crate::StreamId;
use crate::chunk::chunk_payload_data::{ChunkPayloadData, PayloadProtocolIdentifier};
use crate::error::{Error, Result};
use crate::util::*;

use alloc::collections::VecDeque;
use alloc::vec::Vec;
use bytes::{Bytes, BytesMut};
use core::cmp::Ordering;

fn sort_chunks_by_tsn(c: &mut [ChunkPayloadData]) {
    c.sort_by(|a, b| {
        if sna32lt(a.tsn, b.tsn) {
            Ordering::Less
        } else {
            Ordering::Greater
        }
    });
}

fn sort_chunks_by_ssn(c: &mut [Chunks]) {
    c.sort_by(|a, b| {
        if sna16lt(a.ssn, b.ssn) {
            Ordering::Less
        } else {
            Ordering::Greater
        }
    });
}

/// A chunk of data from the stream
#[derive(Debug, PartialEq)]
pub struct Chunk {
    /// The contents of the chunk
    pub bytes: Bytes,
}

/// Chunks is a set of chunks that share the same SSN
#[derive(Default, Debug, Clone)]
pub struct Chunks {
    /// Used only with classic ordered DATA chunks.
    pub(crate) ssn: u16,
    /// Used only with RFC 8260 I-DATA chunks.
    pub(crate) mid: u32,
    pub(crate) interleaved: bool,
    pub ppi: PayloadProtocolIdentifier,
    pub chunks: Vec<ChunkPayloadData>,
    offset: usize,
    index: usize,
}

impl Chunks {
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        let mut l = 0;
        for c in &self.chunks {
            l += c.user_data.len();
        }
        l
    }

    // Concat all fragments into the buffer
    pub fn read(&self, buf: &mut [u8]) -> Result<usize> {
        let mut n_written = 0;
        for c in &self.chunks {
            let to_copy = c.user_data.len();
            let n = core::cmp::min(to_copy, buf.len() - n_written);
            buf[n_written..n_written + n].copy_from_slice(&c.user_data[..n]);
            n_written += n;
            if n < to_copy {
                return Err(Error::ErrShortBuffer);
            }
        }
        Ok(n_written)
    }

    pub fn next(&mut self, max_length: usize) -> Option<Chunk> {
        if self.index >= self.chunks.len() {
            return None;
        }

        let mut buf = BytesMut::with_capacity(max_length);

        let mut n_written = 0;
        while self.index < self.chunks.len() {
            let to_copy = self.chunks[self.index].user_data[self.offset..].len();
            let n = core::cmp::min(to_copy, max_length - n_written);
            buf.extend_from_slice(&self.chunks[self.index].user_data[self.offset..self.offset + n]);
            n_written += n;
            if n < to_copy {
                self.offset += n;
                return Some(Chunk {
                    bytes: buf.freeze(),
                });
            }
            self.index += 1;
            self.offset = 0;
        }

        Some(Chunk {
            bytes: buf.freeze(),
        })
    }

    pub(crate) fn new(
        ssn: u16,
        ppi: PayloadProtocolIdentifier,
        chunks: Vec<ChunkPayloadData>,
    ) -> Self {
        Chunks {
            ssn,
            mid: 0,
            interleaved: false,
            ppi,
            chunks,
            offset: 0,
            index: 0,
        }
    }

    pub(crate) fn new_i_data(
        mid: u32,
        ppi: PayloadProtocolIdentifier,
        chunks: Vec<ChunkPayloadData>,
    ) -> Self {
        Chunks {
            ssn: 0,
            mid,
            interleaved: true,
            ppi,
            chunks,
            offset: 0,
            index: 0,
        }
    }

    pub(crate) fn push(&mut self, chunk: ChunkPayloadData) -> bool {
        // check if dup
        for c in &self.chunks {
            if c.tsn == chunk.tsn {
                return false;
            }
        }

        // append and sort
        self.chunks.push(chunk);
        sort_chunks_by_tsn(&mut self.chunks);

        // Check if we now have a complete set
        self.is_complete()
    }

    pub(crate) fn push_i_data(&mut self, chunk: ChunkPayloadData) -> bool {
        if self
            .chunks
            .iter()
            .any(|c| c.fragment_sequence_number == chunk.fragment_sequence_number)
        {
            return false;
        }
        self.chunks.push(chunk);
        self.chunks.sort_by_key(|c| c.fragment_sequence_number);
        self.is_complete()
    }

    pub(crate) fn is_complete(&self) -> bool {
        // Condition for complete set
        //   0. Has at least one chunk.
        //   1. Begins with beginningFragment set to true
        //   2. Ends with endingFragment set to true
        //   3. TSN monotinically increase by 1 from beginning to end

        // 0.
        let n_chunks = self.chunks.len();
        if n_chunks == 0 {
            return false;
        }

        // 1.
        if !self.chunks[0].beginning_fragment {
            return false;
        }

        // 2.
        if !self.chunks[n_chunks - 1].ending_fragment {
            return false;
        }

        // I-DATA fragments are deliberately permitted to have non-consecutive
        // TSNs; completeness is defined by the FSN sequence instead.
        if self.interleaved {
            return self.chunks[0].fragment_sequence_number == 0
                && self
                    .chunks
                    .iter()
                    .enumerate()
                    .all(|(index, c)| c.fragment_sequence_number == index as u32);
        }

        // Classic DATA fragments must have contiguous TSNs.
        let mut last_tsn = 0u32;
        for (i, c) in self.chunks.iter().enumerate() {
            if i > 0 && c.tsn != last_tsn.wrapping_add(1) {
                return false;
            }
            last_tsn = c.tsn;
        }

        true
    }

    fn is_missing_tsn_at_or_before(&self, cumulative_tsn: u32) -> bool {
        let Some(first) = self.chunks.first() else {
            return false;
        };
        if !first.beginning_fragment && sna32lte(first.tsn.wrapping_sub(1), cumulative_tsn) {
            return true;
        }
        if self.chunks.windows(2).any(|pair| {
            let missing_tsn = pair[0].tsn.wrapping_add(1);
            missing_tsn != pair[1].tsn && sna32lte(missing_tsn, cumulative_tsn)
        }) {
            return true;
        }

        let last = self.chunks.last().expect("a first chunk exists");
        !last.ending_fragment && sna32lte(last.tsn.wrapping_add(1), cumulative_tsn)
    }
}

#[derive(Default, Debug)]
pub(crate) struct ReassemblyQueue {
    pub(crate) si: StreamId,
    pub(crate) next_ssn: u16,
    /// expected SSN for next ordered chunk
    pub(crate) ordered: VecDeque<Chunks>,
    pub(crate) unordered: VecDeque<Chunks>,
    pub(crate) unordered_chunks: Vec<ChunkPayloadData>,
    /// I-DATA uses MID/FSN rather than TSN adjacency for reassembly.
    pub(crate) next_mid: u32,
    pub(crate) ordered_i_data: VecDeque<Chunks>,
    pub(crate) unordered_i_data: VecDeque<Chunks>,
    pub(crate) n_bytes: usize,
    pub(crate) max_message_size: u32,
}

impl ReassemblyQueue {
    /// From RFC 4960 Sec 6.5:
    ///   The Stream Sequence Number in all the streams MUST start from 0 when
    ///   the association is Established.  Also, when the Stream Sequence
    ///   Number reaches the value 65535 the next Stream Sequence Number MUST
    ///   be set to 0.
    pub(crate) fn new(si: StreamId, max_message_size: u32) -> Self {
        ReassemblyQueue {
            si,
            next_ssn: 0, // From RFC 4960 Sec 6.5:
            ordered: VecDeque::new(),
            unordered: VecDeque::new(),
            unordered_chunks: vec![],
            next_mid: 0,
            ordered_i_data: VecDeque::new(),
            unordered_i_data: VecDeque::new(),
            n_bytes: 0,
            max_message_size,
        }
    }

    pub(crate) fn push(&mut self, chunk: ChunkPayloadData) -> Result<bool> {
        if chunk.stream_identifier != self.si {
            return Ok(false);
        }
        if chunk.interleaved {
            return self.push_i_data(chunk);
        }

        if chunk.unordered {
            // Check if adding this chunk would exceed limit for unordered
            let projected_size = self.calculate_unordered_message_size(&chunk);
            if projected_size > self.max_message_size as usize {
                return Err(Error::ErrInboundPacketTooLarge);
            }

            self.n_bytes += chunk.user_data.len();
            self.unordered_chunks.push(chunk);
            sort_chunks_by_tsn(&mut self.unordered_chunks);

            if let Some(cset) = self.find_complete_unordered_chunk_set() {
                self.unordered.push_back(cset);
                return Ok(true);
            }

            Ok(false)
        } else {
            let projected_size = self.calculate_ordered_message_size(&chunk);
            if projected_size > self.max_message_size as usize {
                return Err(Error::ErrInboundPacketTooLarge);
            }
            if sna16lt(chunk.stream_sequence_number, self.next_ssn) {
                return Ok(false);
            }

            self.n_bytes += chunk.user_data.len();
            for s in &mut self.ordered {
                if s.ssn == chunk.stream_sequence_number {
                    return Ok(s.push(chunk));
                }
            }

            let mut cset = Chunks::new(chunk.stream_sequence_number, chunk.payload_type, vec![]);
            let ok = cset.push(chunk);
            self.ordered.push_back(cset);
            sort_chunks_by_ssn(self.ordered.make_contiguous());
            Ok(ok)
        }
    }

    fn push_i_data(&mut self, chunk: ChunkPayloadData) -> Result<bool> {
        let queue = if chunk.unordered {
            &mut self.unordered_i_data
        } else {
            &mut self.ordered_i_data
        };
        if !chunk.unordered && sna32lt(chunk.message_identifier, self.next_mid) {
            return Ok(false);
        }

        let mid = chunk.message_identifier;
        let projected = queue
            .iter()
            .find(|set| set.mid == mid)
            .map_or(0, Chunks::len)
            + chunk.user_data.len();
        if projected > self.max_message_size as usize {
            return Err(Error::ErrInboundPacketTooLarge);
        }

        if let Some(set) = queue.iter_mut().find(|set| set.mid == mid) {
            let accepted = !set
                .chunks
                .iter()
                .any(|c| c.fragment_sequence_number == chunk.fragment_sequence_number);
            if accepted {
                self.n_bytes += chunk.user_data.len();
            }
            return Ok(set.push_i_data(chunk));
        }

        let ppi = chunk.payload_type;
        let mut set = Chunks::new_i_data(mid, ppi, vec![]);
        let complete = set.push_i_data(chunk);
        self.n_bytes += set.len();
        queue.push_back(set);
        if !queue.front().is_some_and(|set| set.mid == mid) {
            let mut sets: Vec<_> = queue.drain(..).collect();
            sets.sort_by(|a, b| {
                if sna32lt(a.mid, b.mid) {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            });
            *queue = sets.into();
        }
        Ok(complete)
    }

    fn calculate_ordered_message_size(&self, new_chunk: &ChunkPayloadData) -> usize {
        let ssn = new_chunk.stream_sequence_number;
        let existing: usize = self
            .ordered
            .iter()
            .find(|s| s.ssn == ssn)
            .map(|s| s.len())
            .unwrap_or(0);
        existing + new_chunk.user_data.len()
    }

    fn calculate_unordered_message_size(&self, new_chunk: &ChunkPayloadData) -> usize {
        // For unordered, calculate size of contiguous chunk set this belongs to
        // This is more complex - need to find the message boundary

        // first find the set of TSNs that preceeds this chunk
        let prefix = if new_chunk.beginning_fragment {
            0
        } else if let Some(mut p) = self
            .unordered_chunks
            .iter()
            .rposition(|f| f.tsn == new_chunk.tsn.wrapping_sub(1))
        {
            let mut cnt = 0;
            let mut tsn = new_chunk.tsn;
            loop {
                if self.unordered_chunks[p].tsn == tsn.wrapping_sub(1) {
                    cnt += self.unordered_chunks[p].user_data.len();
                    tsn = self.unordered_chunks[p].tsn;
                } else {
                    break;
                }

                if self.unordered_chunks[p].beginning_fragment || (p == 0) {
                    break;
                }
                p -= 1;
            }
            cnt
        } else {
            0
        };

        // next find the set of TSNs that succeeds this chunk
        let suffix = if new_chunk.ending_fragment {
            0
        } else if let Some(mut p) = self
            .unordered_chunks
            .iter()
            .rposition(|f| f.tsn == new_chunk.tsn.wrapping_add(1))
        {
            let mut cnt = 0;
            let mut tsn = new_chunk.tsn;
            while p < self.unordered_chunks.len() {
                if self.unordered_chunks[p].tsn == tsn.wrapping_add(1) {
                    cnt += self.unordered_chunks[p].user_data.len();
                    tsn = self.unordered_chunks[p].tsn;
                } else {
                    break;
                }

                if self.unordered_chunks[p].ending_fragment {
                    break;
                }
                p += 1;
            }
            cnt
        } else {
            0
        };

        // now sum the lengths together
        prefix + new_chunk.user_data.len() + suffix
    }

    pub(crate) fn find_complete_unordered_chunk_set(&mut self) -> Option<Chunks> {
        let mut start_idx = -1isize;
        let mut n_chunks = 0usize;
        let mut last_tsn = 0u32;
        let mut found = false;

        for (i, c) in self.unordered_chunks.iter().enumerate() {
            // seek beginning
            if c.beginning_fragment {
                start_idx = i as isize;
                n_chunks = 1;
                last_tsn = c.tsn;

                if c.ending_fragment {
                    found = true;
                    break;
                }
                continue;
            }

            if start_idx < 0 {
                continue;
            }

            // Check if contiguous in TSN
            if c.tsn != last_tsn.wrapping_add(1) {
                start_idx = -1;
                continue;
            }

            last_tsn = c.tsn;
            n_chunks += 1;

            if c.ending_fragment {
                found = true;
                break;
            }
        }

        if !found {
            return None;
        }

        // Extract the range of chunks
        let chunks: Vec<ChunkPayloadData> = self
            .unordered_chunks
            .drain(start_idx as usize..(start_idx as usize) + n_chunks)
            .collect();
        Some(Chunks::new(0, chunks[0].payload_type, chunks))
    }

    pub(crate) fn is_readable(&self) -> bool {
        if self
            .unordered_i_data
            .front()
            .is_some_and(Chunks::is_complete)
        {
            return true;
        }
        if self
            .ordered_i_data
            .front()
            .is_some_and(|set| set.is_complete() && sna32lte(set.mid, self.next_mid))
        {
            return true;
        }
        if !self.unordered.is_empty() {
            return true;
        }
        self.ordered
            .front()
            .is_some_and(|set| set.is_complete() && sna16lte(set.ssn, self.next_ssn))
    }

    pub(crate) fn read(&mut self) -> Option<Chunks> {
        let chunks = if self
            .unordered_i_data
            .front()
            .is_some_and(Chunks::is_complete)
        {
            self.unordered_i_data.pop_front()?
        } else if let Some(set) = self.ordered_i_data.front() {
            if !set.is_complete() || sna32gt(set.mid, self.next_mid) {
                return None;
            }
            let mid = set.mid;
            let result = self.ordered_i_data.pop_front()?;
            if mid == self.next_mid {
                self.next_mid = self.next_mid.wrapping_add(1);
            }
            result
        } else if !self.unordered.is_empty() {
            self.unordered.pop_front()?
        } else if let Some(set) = self.ordered.front() {
            if !set.is_complete() || sna16gt(set.ssn, self.next_ssn) {
                return None;
            }
            let ssn = set.ssn;
            let result = self.ordered.pop_front()?;
            if ssn == self.next_ssn {
                self.next_ssn = self.next_ssn.wrapping_add(1);
            }
            result
        } else {
            return None;
        };

        self.subtract_num_bytes(chunks.len());
        Some(chunks)
    }

    /// Use last_ssn to locate a chunkSet then remove it if the set has
    /// not been complete
    pub(crate) fn forward_tsn_for_ordered(&mut self, last_ssn: u16) {
        let num_bytes = self
            .ordered
            .iter()
            .filter(|s| sna16lte(s.ssn, last_ssn) && !s.is_complete())
            .fold(0, |n, s| {
                n + s.chunks.iter().fold(0, |acc, c| acc + c.user_data.len())
            });
        self.subtract_num_bytes(num_bytes);

        self.ordered
            .retain(|s| !sna16lte(s.ssn, last_ssn) || s.is_complete());

        // Finally, forward next_ssn
        if sna16lte(self.next_ssn, last_ssn) {
            self.next_ssn = last_ssn.wrapping_add(1);
        }
    }

    /// Apply an ordered Forward-TSN retained across a stream reset without
    /// crossing into messages whose TSNs are newer than its cumulative point.
    pub(crate) fn forward_tsn_for_ordered_bounded(
        &mut self,
        last_ssn: u16,
        new_cumulative_tsn: u32,
    ) {
        let first_unaffected_ssn = self
            .ordered
            .iter()
            .filter(|chunks| {
                sna16lte(chunks.ssn, last_ssn)
                    && if chunks.is_complete() {
                        chunks
                            .chunks
                            .iter()
                            .any(|chunk| sna32gt(chunk.tsn, new_cumulative_tsn))
                    } else {
                        !chunks.is_missing_tsn_at_or_before(new_cumulative_tsn)
                    }
            })
            .map(|chunks| chunks.ssn)
            .reduce(|first, candidate| {
                if sna16lt(candidate, first) {
                    candidate
                } else {
                    first
                }
            });

        let is_abandoned_partial = |chunks: &&Chunks| {
            sna16lte(chunks.ssn, last_ssn)
                && !chunks.is_complete()
                && chunks.is_missing_tsn_at_or_before(new_cumulative_tsn)
        };
        let num_bytes = self
            .ordered
            .iter()
            .filter(is_abandoned_partial)
            .flat_map(|chunks| chunks.chunks.iter())
            .map(|chunk| chunk.user_data.len())
            .sum();
        self.subtract_num_bytes(num_bytes);
        self.ordered.retain(|chunks| {
            !sna16lte(chunks.ssn, last_ssn)
                || chunks.is_complete()
                || !chunks.is_missing_tsn_at_or_before(new_cumulative_tsn)
        });

        let next_ssn = first_unaffected_ssn.unwrap_or_else(|| last_ssn.wrapping_add(1));
        if sna16lt(self.next_ssn, next_ssn) {
            self.next_ssn = next_ssn;
        }
    }

    /// The highest prefix of an ambiguous deferred ordered skip that has
    /// enough TSN context to advance without overtaking a missing successor.
    pub(crate) fn applicable_forward_tsn_for_ordered_bounded(
        &self,
        last_ssn: u16,
        new_cumulative_tsn: u32,
        peer_last_tsn: u32,
    ) -> Option<u16> {
        if sna16gt(self.next_ssn, last_ssn) {
            return Some(last_ssn);
        }

        let is_covered = |chunks: &Chunks| {
            if chunks.is_complete() {
                chunks
                    .chunks
                    .iter()
                    .all(|chunk| sna32lte(chunk.tsn, new_cumulative_tsn))
            } else {
                chunks.is_missing_tsn_at_or_before(new_cumulative_tsn)
            }
        };
        if self.ordered.iter().any(|chunks| {
            chunks.ssn == last_ssn && (chunks.ssn == self.next_ssn || is_covered(chunks))
        }) {
            return Some(last_ssn);
        }

        let has_resolved_later_tsn = self
            .ordered
            .iter()
            .filter(|chunks| chunks.ssn == last_ssn || sna16gt(chunks.ssn, last_ssn))
            .flat_map(|chunks| chunks.chunks.iter())
            .map(|chunk| chunk.tsn)
            .reduce(|first, candidate| {
                if sna32lt(candidate, first) {
                    candidate
                } else {
                    first
                }
            })
            .is_some_and(|first_tsn| sna32lte(first_tsn, peer_last_tsn));
        if has_resolved_later_tsn {
            return Some(last_ssn);
        }

        self.ordered
            .iter()
            .filter(|chunks| {
                (chunks.ssn == self.next_ssn || sna16gt(chunks.ssn, self.next_ssn))
                    && sna16lt(chunks.ssn, last_ssn)
                    && (is_covered(chunks)
                        || chunks
                            .chunks
                            .first()
                            .is_some_and(|chunk| sna32lte(chunk.tsn, peer_last_tsn)))
            })
            .map(|chunks| chunks.ssn)
            .reduce(|last, candidate| {
                if sna16gt(candidate, last) {
                    candidate
                } else {
                    last
                }
            })
    }

    /// Remove all fragments in the unordered sets that contains chunks
    /// equal to or older than `new_cumulative_tsn`.
    /// We know all sets in the r.unordered are complete ones.
    /// Just remove chunks that are equal to or older than new_cumulative_tsn
    /// from the unordered_chunks
    pub(crate) fn forward_tsn_for_unordered(&mut self, new_cumulative_tsn: u32) {
        let mut last_idx: isize = -1;
        for (i, c) in self.unordered_chunks.iter().enumerate() {
            if sna32gt(c.tsn, new_cumulative_tsn) {
                break;
            }
            last_idx = i as isize;
        }
        if last_idx >= 0 {
            for i in 0..(last_idx + 1) as usize {
                self.subtract_num_bytes(self.unordered_chunks[i].user_data.len());
            }
            self.unordered_chunks.drain(..(last_idx + 1) as usize);
        }
    }

    /// Apply RFC 8260's ordered I-FORWARD-TSN MID update.
    pub(crate) fn forward_i_tsn_for_ordered(&mut self, last_mid: u32) {
        let dropped: usize = self
            .ordered_i_data
            .iter()
            .filter(|set| sna32lte(set.mid, last_mid) && !set.is_complete())
            .map(Chunks::len)
            .sum();
        self.ordered_i_data
            .retain(|set| !sna32lte(set.mid, last_mid) || set.is_complete());
        self.subtract_num_bytes(dropped);
        if sna32lte(self.next_mid, last_mid) {
            self.next_mid = last_mid.wrapping_add(1);
        }
    }

    /// Drop incomplete unordered I-DATA messages skipped by I-FORWARD-TSN.
    pub(crate) fn forward_i_tsn_for_unordered(&mut self, last_mid: u32) {
        let dropped: usize = self
            .unordered_i_data
            .iter()
            .filter(|set| sna32lte(set.mid, last_mid) && !set.is_complete())
            .map(Chunks::len)
            .sum();
        self.unordered_i_data
            .retain(|set| !sna32lte(set.mid, last_mid) || set.is_complete());
        self.subtract_num_bytes(dropped);
    }

    pub(crate) fn subtract_num_bytes(&mut self, n_bytes: usize) {
        if self.n_bytes >= n_bytes {
            self.n_bytes -= n_bytes;
        } else {
            self.n_bytes = 0;
        }
    }

    pub(crate) fn get_num_bytes(&self) -> usize {
        self.n_bytes
    }
}
