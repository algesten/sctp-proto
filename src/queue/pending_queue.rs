use crate::chunk::chunk_payload_data::ChunkPayloadData;

use alloc::collections::VecDeque;
use core::time::Duration;
use std::time::Instant;

/// pendingBaseQueue
pub(crate) type PendingBaseQueue = VecDeque<ChunkPayloadData>;

/// State used by classic DATA scheduling, which keeps one fragmented message
/// selected until its final fragment is transmitted.
#[derive(Debug, Default)]
struct ClassicScheduler {
    selected: bool,
    unordered_is_selected: bool,
}

/// State used by RFC 8260 scheduling, which rotates a message's unsent tail
/// after every fragment to permit other messages to make progress.
#[derive(Debug, Default)]
struct InterleavingScheduler {
    prefer_unordered: bool,
}

#[derive(Debug)]
enum Scheduler {
    Classic(ClassicScheduler),
    Interleaving(InterleavingScheduler),
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::Classic(ClassicScheduler::default())
    }
}

/// pendingQueue
#[derive(Debug, Default)]
pub(crate) struct PendingQueue {
    unordered_queue: PendingBaseQueue,
    ordered_queue: PendingBaseQueue,
    queue_len: usize,
    n_bytes: usize,
    scheduler: Scheduler,
}

impl PendingQueue {
    pub(crate) fn new() -> Self {
        PendingQueue::default()
    }

    pub(crate) fn set_interleaving(&mut self, enabled: bool) {
        self.scheduler = if enabled {
            Scheduler::Interleaving(InterleavingScheduler::default())
        } else {
            Scheduler::Classic(ClassicScheduler::default())
        };
    }

    pub(crate) fn push(&mut self, c: ChunkPayloadData) {
        self.n_bytes += c.user_data.len();
        if c.unordered {
            self.unordered_queue.push_back(c);
        } else {
            self.ordered_queue.push_back(c);
        }
        self.queue_len += 1;
    }

    /// Start lifetimes for the newly queued suffix using the next transmit poll's time.
    /// Both queues retain message-ID order, so old pending messages need no rescan.
    pub(crate) fn start_timed_messages(&mut self, first_message_id: u64, now: Instant) {
        for queue in [&mut self.ordered_queue, &mut self.unordered_queue] {
            for chunk in queue
                .iter_mut()
                .rev()
                .take_while(|chunk| chunk.message_id >= first_message_id)
            {
                if chunk.deadline.is_some()
                    || chunk.payload_type == crate::PayloadProtocolIdentifier::Dcep
                {
                    continue;
                }
                if let Some((crate::ReliabilityType::Timed, lifetime)) = chunk.reliability {
                    chunk.deadline = now.checked_add(Duration::from_millis(u64::from(lifetime)));
                }
            }
        }
    }

    pub(crate) fn peek(&self) -> Option<&ChunkPayloadData> {
        match &self.scheduler {
            Scheduler::Classic(state) if state.selected => {
                if state.unordered_is_selected {
                    self.unordered_queue.front()
                } else {
                    self.ordered_queue.front()
                }
            }
            Scheduler::Classic(_) => self.unordered_queue.front().or(self.ordered_queue.front()),
            Scheduler::Interleaving(state) => {
                match (self.unordered_queue.front(), self.ordered_queue.front()) {
                    (Some(unordered), Some(_)) if state.prefer_unordered => Some(unordered),
                    (Some(_), Some(ordered)) => Some(ordered),
                    (Some(unordered), None) => Some(unordered),
                    (None, Some(ordered)) => Some(ordered),
                    (None, None) => None,
                }
            }
        }
    }

    pub(crate) fn pop(
        &mut self,
        beginning_fragment: bool,
        unordered: bool,
    ) -> Option<ChunkPayloadData> {
        if matches!(self.scheduler, Scheduler::Interleaving(_)) {
            let queue = if unordered {
                &mut self.unordered_queue
            } else {
                &mut self.ordered_queue
            };
            let popped = queue.pop_front()?;
            let message_id = popped.message_id;
            while queue
                .front()
                .is_some_and(|chunk| chunk.message_id == message_id)
            {
                if let Some(chunk) = queue.pop_front() {
                    queue.push_back(chunk);
                }
            }
            if let Scheduler::Interleaving(state) = &mut self.scheduler {
                state.prefer_unordered = !unordered;
            }
            self.n_bytes -= popped.user_data.len();
            self.queue_len -= 1;
            return Some(popped);
        }

        let Scheduler::Classic(state) = &mut self.scheduler else {
            unreachable!("interleaving scheduler returned above");
        };
        let popped = if state.selected {
            let popped = if state.unordered_is_selected {
                self.unordered_queue.pop_front()
            } else {
                self.ordered_queue.pop_front()
            };
            if let Some(p) = &popped {
                if p.ending_fragment {
                    state.selected = false;
                }
            }
            popped
        } else {
            if !beginning_fragment {
                return None;
            }
            if unordered {
                let popped = self.unordered_queue.pop_front();
                if let Some(p) = &popped {
                    if !p.ending_fragment {
                        state.selected = true;
                        state.unordered_is_selected = true;
                    }
                }
                popped
            } else {
                let popped = self.ordered_queue.pop_front();
                if let Some(p) = &popped {
                    if !p.ending_fragment {
                        state.selected = true;
                        state.unordered_is_selected = false;
                    }
                }
                popped
            }
        };

        if let Some(p) = &popped {
            self.n_bytes -= p.user_data.len();
            self.queue_len -= 1;
        }

        popped
    }

    /// Close the SSN gap after removing an entirely unsent ordered message.
    pub(crate) fn compact_ordered_ssns(&mut self, stream_identifier: u16) {
        for chunk in &mut self.ordered_queue {
            if chunk.stream_identifier == stream_identifier {
                chunk.stream_sequence_number = chunk.stream_sequence_number.wrapping_sub(1);
            }
        }
    }

    pub(crate) fn get_num_bytes(&self) -> usize {
        self.n_bytes
    }

    /// Remove a message without allocating a temporary collection. Selection
    /// persists when removing a different message from the pending queues.
    pub(crate) fn remove_message(
        &mut self,
        message_id: u64,
        mut on_remove: impl FnMut(ChunkPayloadData),
    ) -> usize {
        let selected_message = self.peek().map(|chunk| chunk.message_id);
        if let Scheduler::Classic(state) = &mut self.scheduler
            && state.selected
            && selected_message == Some(message_id)
        {
            state.selected = false;
        }
        let mut removed_bytes = 0;
        let mut removed_chunks = 0;
        for queue in [&mut self.ordered_queue, &mut self.unordered_queue] {
            queue.retain_mut(|chunk| {
                if chunk.message_id != message_id {
                    return true;
                }
                removed_bytes += chunk.user_data.len();
                removed_chunks += 1;
                on_remove(core::mem::take(chunk));
                false
            });
        }
        self.n_bytes -= removed_bytes;
        self.queue_len -= removed_chunks;
        removed_bytes
    }

    pub(crate) fn len(&self) -> usize {
        self.queue_len
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn contains_message(&self, message_id: u64) -> bool {
        self.unordered_queue
            .iter()
            .chain(self.ordered_queue.iter())
            .any(|chunk| chunk.message_id == message_id)
    }

    pub(crate) fn contains_stream(&self, stream_identifier: u16) -> bool {
        self.unordered_queue
            .iter()
            .chain(self.ordered_queue.iter())
            .any(|chunk| chunk.stream_identifier == stream_identifier)
    }
}
