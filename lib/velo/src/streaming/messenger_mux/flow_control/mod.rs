// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Credit bookkeeping for the messenger mux, per `BATCHING.md` § "Flow
//! control".
//!
//! Pure and synchronous. Two ledgers, because the two sides of a slot keep
//! different books:
//!
//! - [`SlotCredit`] — **egress**. What this side may still *send* on a slot.
//!   Spends on every record; grows on an inbound `CreditUpdate`.
//! - [`SlotCreditAccount`] — **ingress**. What this side has *granted* and what
//!   is sitting in the mux-owned slot buffer. Admits on apply, releases as
//!   `reader_pump` drains, and hands back the delta to advertise.
//!
//! Both are gated by [`CreditClass`], which is where the two reservations live:
//! one terminal credit spendable at most once, and control records that data
//! exhaustion can never block.
//!
//! The shared resource this protects is not a socket, it is the peer's
//! **ordering lane**. A `_stream_batch` handler that awaits holds that lane and
//! stalls every slot from that peer — head-of-line blocking with a worse
//! failure mode than a socket's, because lane channels are unbounded and a
//! blocking ingress converts backpressure into unbounded memory growth. So
//! ingress is nonblocking, and these types are what make that safe.
//!
//! > **Invariant.** A slot never has more than `C` frames outstanding against a
//! > `C + 1`-deep buffer, so the applier only `try_send`s into space credit
//! > already reserved and never blocks its lane.
//!
//! [`slot_buffer_depth`] is that arithmetic, and
//! [`SlotCreditAccount::buffered`] is the occupancy it bounds.

use super::protocol::RecordType;

/// Per-peer byte budget across all of that peer's slots.
///
/// Frame-count credit alone bounds memory at `slots × C × max frame size` — a
/// meaningless number. Today the kernel socket enforces a real ~1 MiB-per-stream
/// limit for free; riding the Messenger deletes exactly that protection,
/// because the socket is now shared with control traffic and is not per-stream
/// at all. Frame credit gives the no-head-of-line-blocking proof, byte credit
/// the memory bound — different jobs, both needed.
pub(crate) const DEFAULT_PEER_BYTE_BUDGET: u64 = 8 * 1024 * 1024;

/// Per-slot byte cap, the replacement for the ~1 MiB the kernel socket used to
/// enforce per stream for free. Also the value substituted when a peer
/// advertises `slot_byte_budget = 0`.
pub(crate) const DEFAULT_SLOT_BYTE_BUDGET: u32 = 1024 * 1024;

/// Depth a mux-owned slot buffer must have: `C` data credits plus the one
/// reserved terminal credit.
///
/// This is the whole "applier never blocks its lane" proof in one line. Credit
/// is issued against *this* buffer and never against the anchor's `frame_tx`,
/// which has writers other than the mux — the local same-worker attach path,
/// detach and finalize, `reader_pump`'s own watchdog injection, and decisively
/// M concurrent MPSC senders. Any "C credits against a C-deep channel" proof
/// collapses the moment a second writer exists.
///
/// The `u32 → usize` widening makes the `+ 1` unrepresentable as an overflow on
/// every target this crate builds for.
pub(crate) const fn slot_buffer_depth(data_credit: u32) -> usize {
    data_credit as usize + 1
}

// ---------------------------------------------------------------------------
// Negotiated limits
// ---------------------------------------------------------------------------

/// Why a peer's advertised limits cannot drive the mux.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum NegotiationError {
    /// `initial_credit = 0`. Both attach-response fields are
    /// `#[serde(default)]` so an older peer still deserializes — as one
    /// advertising zero credit. There is no safe default to invent here: a
    /// sender that guessed would push into a buffer the receiver never sized.
    #[error("peer advertised initial_credit = 0: legacy peer, the mux is unusable")]
    LegacyPeer,
}

/// Credit limits agreed at attach time.
///
/// MPSC anchor capacity is caller-configurable, so the receiver advertises what
/// it can absorb rather than both sides assuming a constant.
///
/// The two zero values do **not** mean the same thing, and `BATCHING.md` is the
/// authority on the difference: `initial_credit = 0` is a legacy peer and the
/// mux is unusable, while `slot_byte_budget = 0` merely means "use the
/// default".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct NegotiatedLimits {
    initial_credit: u32,
    slot_byte_budget: u32,
}

impl NegotiatedLimits {
    /// Interprets the two `#[serde(default)]` attach-response fields.
    pub(crate) const fn from_wire(
        initial_credit: u32,
        slot_byte_budget: u32,
    ) -> Result<Self, NegotiationError> {
        if initial_credit == 0 {
            return Err(NegotiationError::LegacyPeer);
        }
        let slot_byte_budget = if slot_byte_budget == 0 {
            DEFAULT_SLOT_BYTE_BUDGET
        } else {
            slot_byte_budget
        };
        Ok(Self {
            initial_credit,
            slot_byte_budget,
        })
    }

    /// Data credit `C` granted to each new slot.
    pub(crate) const fn initial_credit(&self) -> u32 {
        self.initial_credit
    }

    /// Bytes one slot may hold in flight.
    pub(crate) const fn slot_byte_budget(&self) -> u32 {
        self.slot_byte_budget
    }

    /// `C + 1` — the depth `bind_muxed` sizes its receiver to.
    pub(crate) const fn slot_buffer_depth(&self) -> usize {
        slot_buffer_depth(self.initial_credit)
    }

    /// An egress ledger opened at these limits.
    pub(crate) const fn open_credit(&self) -> SlotCredit {
        SlotCredit::new(self.initial_credit)
    }

    /// An ingress account opened at these limits.
    pub(crate) const fn open_account(&self) -> SlotCreditAccount {
        SlotCreditAccount::new(self.initial_credit)
    }
}

// ---------------------------------------------------------------------------
// Credit classes
// ---------------------------------------------------------------------------

/// Which reservation a record spends from.
///
/// Classifying once, here, is what keeps the two reservations from drifting
/// apart between the sender ledger and the receiver account.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum CreditClass {
    /// Ordinary payload. Spends one of the `C` data credits.
    Data,
    /// A record matching `is_terminal_sentinel`. Spends the single reserved
    /// terminal credit, which `sent_terminal` guarantees is enough: at most one
    /// terminal per slot, after which the slot closes.
    ///
    /// `SenderError` is deliberately *not* terminal and correctly spends data
    /// credit.
    Terminal,
    /// `OpenSlot`, `CloseSlot`, `CreditUpdate`. Never blocked by data credit
    /// exhaustion, so control is never what a starved slot fails to deliver —
    /// including the `CloseSlot` that ends it.
    Control,
}

impl CreditClass {
    /// Classifies a record.
    ///
    /// `is_terminal` is the caller's `is_terminal_sentinel(body)` verdict; only
    /// a `Data` record can carry one.
    ///
    /// `SlotHeartbeat` is deliberately **not** control. A heartbeat dropped
    /// under saturation *is* the per-slot saturation signal — the one thing a
    /// streaming beat still uniquely carries now that the Messenger detects
    /// process, host and connection death itself. Give it a reserve and
    /// `reader_pump`'s `DETECTION_MULTIPLIER` stops firing on a saturated slot.
    pub(crate) const fn of(record_type: RecordType, is_terminal: bool) -> Self {
        match record_type {
            RecordType::OpenSlot | RecordType::CloseSlot | RecordType::CreditUpdate => {
                Self::Control
            }
            RecordType::Data if is_terminal => Self::Terminal,
            RecordType::Data | RecordType::SlotHeartbeat => Self::Data,
        }
    }

    /// Whether this class occupies a place in the `C + 1` slot buffer.
    ///
    /// Control records do not: `OpenSlot` and `CloseSlot` act on the registry
    /// and `CreditUpdate` feeds the egress ledger, so none is ever handed to a
    /// consumer. That is precisely why the buffer cannot overflow.
    pub(crate) const fn occupies_buffer(self) -> bool {
        !matches!(self, Self::Control)
    }
}

/// Why a record cannot be admitted against a slot's credit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum CreditError {
    /// No data credit left. On egress the slot parks until a `CreditUpdate`
    /// arrives; on ingress it is a peer overspending its grant — a protocol
    /// error scoped to that slot.
    #[error("slot data credit exhausted")]
    DataExhausted,
    /// A second terminal for one slot. Unreachable through `sent_terminal`, so
    /// on ingress this is a malformed peer.
    #[error("slot terminal reserve already spent")]
    TerminalAlreadySpent,
}

/// The single terminal credit each slot holds back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TerminalReserve {
    Unspent,
    Spent,
}

// ---------------------------------------------------------------------------
// Egress ledger
// ---------------------------------------------------------------------------

/// Sender-side per-slot credit: what this side may still put on the wire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SlotCredit {
    data_available: u32,
    terminal: TerminalReserve,
}

impl SlotCredit {
    /// Opens a ledger holding `initial` data credits and one terminal credit.
    pub(crate) const fn new(initial: u32) -> Self {
        Self {
            data_available: initial,
            terminal: TerminalReserve::Unspent,
        }
    }

    /// Unspent data credit.
    pub(crate) const fn data_available(&self) -> u32 {
        self.data_available
    }

    /// Whether the terminal credit is still held.
    pub(crate) const fn terminal_available(&self) -> bool {
        matches!(self.terminal, TerminalReserve::Unspent)
    }

    /// Whether a record of `class` could be sent right now.
    ///
    /// The non-mutating half of `try_spend`, for the F1 rule: `try_acquire()`,
    /// then on failure `request_flush(Starved)`, and only *then* await.
    pub(crate) const fn can_spend(&self, class: CreditClass) -> bool {
        match class {
            CreditClass::Control => true,
            CreditClass::Terminal => self.terminal_available(),
            CreditClass::Data => self.data_available > 0,
        }
    }

    /// Spends one credit of `class`.
    ///
    /// Control always succeeds and consumes nothing — that is the reserved
    /// control capacity, expressed as the absence of a counter to exhaust.
    pub(crate) fn try_spend(&mut self, class: CreditClass) -> Result<(), CreditError> {
        match class {
            CreditClass::Control => Ok(()),
            CreditClass::Terminal => match self.terminal {
                TerminalReserve::Unspent => {
                    self.terminal = TerminalReserve::Spent;
                    Ok(())
                }
                TerminalReserve::Spent => Err(CreditError::TerminalAlreadySpent),
            },
            CreditClass::Data => match self.data_available.checked_sub(1) {
                Some(remaining) => {
                    self.data_available = remaining;
                    Ok(())
                }
                None => Err(CreditError::DataExhausted),
            },
        }
    }

    /// Applies an inbound `CreditUpdate`, returning the new data credit.
    ///
    /// Saturating: a peer cannot make this ledger wrap, and a grant beyond
    /// `u32::MAX` outstanding frames is meaningless long before it is unsafe.
    pub(crate) fn grant(&mut self, delta: u32) -> u32 {
        self.data_available = self.data_available.saturating_add(delta);
        self.data_available
    }
}

// ---------------------------------------------------------------------------
// Ingress account
// ---------------------------------------------------------------------------

/// Receiver-side per-slot accounting against the mux-owned `C + 1` buffer.
///
/// `admit` is called by the applier before `try_send`; `release` by
/// `reader_pump` after each successful handoff to `frame_tx` — exact, O(1) and
/// immediate, because flume has no consumed-callback, a per-slot drain task
/// would reintroduce the per-stream tasks the mux exists to remove, and polling
/// the receiver's length is only a sampled approximation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SlotCreditAccount {
    limit: u32,
    data_outstanding: u32,
    buffered: u32,
    ungranted: u32,
    terminal: TerminalReserve,
}

impl SlotCreditAccount {
    /// Opens an account granting `limit` data credits.
    pub(crate) const fn new(limit: u32) -> Self {
        Self {
            limit,
            data_outstanding: 0,
            buffered: 0,
            ungranted: 0,
            terminal: TerminalReserve::Unspent,
        }
    }

    /// Data credit `C` granted to the peer.
    pub(crate) const fn limit(&self) -> u32 {
        self.limit
    }

    /// Depth the slot buffer must have for `try_send` to be infallible.
    pub(crate) const fn buffer_depth(&self) -> usize {
        slot_buffer_depth(self.limit)
    }

    /// Records currently occupying the slot buffer.
    ///
    /// Bounded by [`buffer_depth`](Self::buffer_depth) — that bound is the
    /// invariant, and `velo_streaming_mux_reader_stall_total > 0` is what a
    /// break in it looks like from the outside.
    pub(crate) const fn buffered(&self) -> u32 {
        self.buffered
    }

    /// Data credit the peer has spent and not had returned.
    pub(crate) const fn data_outstanding(&self) -> u32 {
        self.data_outstanding
    }

    /// Data credit the peer may still spend.
    pub(crate) const fn data_free(&self) -> u32 {
        self.limit - self.data_outstanding
    }

    /// Credit released but not yet advertised, awaiting a `CreditUpdate`.
    pub(crate) const fn pending_grant(&self) -> u32 {
        self.ungranted
    }

    /// Admits one record of `class` into the slot.
    ///
    /// An `Err` here means the peer overspent what it was granted: the slot is
    /// closed with [`CloseReason::ProtocolError`] and metered. Nothing has been
    /// mutated, so the caller may close without unwinding an accounting change.
    ///
    /// [`CloseReason::ProtocolError`]: super::protocol::CloseReason::ProtocolError
    pub(crate) fn admit(&mut self, class: CreditClass) -> Result<(), CreditError> {
        match class {
            CreditClass::Control => return Ok(()),
            CreditClass::Terminal => match self.terminal {
                TerminalReserve::Unspent => self.terminal = TerminalReserve::Spent,
                TerminalReserve::Spent => return Err(CreditError::TerminalAlreadySpent),
            },
            CreditClass::Data => {
                if self.data_outstanding >= self.limit {
                    return Err(CreditError::DataExhausted);
                }
                self.data_outstanding += 1;
            }
        }
        self.buffered += 1;
        Ok(())
    }

    /// Accounts for `drained` records leaving the buffer, returning the credit
    /// now waiting to be advertised.
    ///
    /// The terminal occupies the `+ 1` and returns no credit when it drains:
    /// the slot closes behind it, so there is nothing left to grant credit to.
    /// Only the data records feed [`pending_grant`](Self::pending_grant).
    pub(crate) fn release(&mut self, drained: u32) -> u32 {
        let leaving = drained.min(self.buffered);
        self.buffered -= leaving;
        let data = leaving.min(self.data_outstanding);
        self.data_outstanding -= data;
        self.ungranted = self.ungranted.saturating_add(data);
        self.ungranted
    }

    /// Takes the pending grant for a `CreditUpdate` record, or `None` when
    /// there is nothing to advertise.
    pub(crate) fn take_pending_grant(&mut self) -> Option<u32> {
        let delta = self.ungranted;
        if delta == 0 {
            return None;
        }
        self.ungranted = 0;
        Some(delta)
    }
}

// ---------------------------------------------------------------------------
// Byte budgets
// ---------------------------------------------------------------------------

/// Why a byte reservation failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ByteBudgetError {
    /// Permanent. The request is larger than the entire budget, so no amount of
    /// draining admits it. The caller must route it another way — the oversized
    /// record goes alone in its batch and rides rendezvous — rather than park.
    #[error("{requested} bytes exceeds the {limit}-byte budget outright")]
    ExceedsBudget { requested: u64, limit: u64 },
    /// Transient. The request fits the budget but not what is left of it. The
    /// caller parks until a [`ByteBudget::release`].
    #[error("{requested} bytes does not fit the {available} bytes left of {limit}")]
    Exhausted {
        requested: u64,
        available: u64,
        limit: u64,
    },
}

impl ByteBudgetError {
    /// Whether draining could ever admit this request.
    pub(crate) const fn is_transient(&self) -> bool {
        matches!(self, Self::Exhausted { .. })
    }
}

/// A saturating byte reservation counter.
///
/// One type serves both scopes: per peer at [`DEFAULT_PEER_BYTE_BUDGET`] and
/// per slot at the negotiated [`NegotiatedLimits::slot_byte_budget`]. Usage
/// never exceeds the limit by construction, so `available` cannot underflow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ByteBudget {
    limit: u64,
    used: u64,
}

impl ByteBudget {
    /// A budget of `limit` bytes.
    pub(crate) const fn new(limit: u64) -> Self {
        Self { limit, used: 0 }
    }

    /// The per-peer budget across all of that peer's slots.
    pub(crate) const fn per_peer() -> Self {
        Self::new(DEFAULT_PEER_BYTE_BUDGET)
    }

    /// The per-slot cap at the negotiated budget.
    pub(crate) const fn per_slot(limits: &NegotiatedLimits) -> Self {
        Self::new(limits.slot_byte_budget() as u64)
    }

    /// The ceiling.
    pub(crate) const fn limit(&self) -> u64 {
        self.limit
    }

    /// Bytes currently reserved.
    pub(crate) const fn used(&self) -> u64 {
        self.used
    }

    /// Bytes still reservable.
    pub(crate) const fn available(&self) -> u64 {
        self.limit - self.used
    }

    /// Reserves `bytes`, or explains which kind of "no" this is.
    pub(crate) fn try_reserve(&mut self, bytes: usize) -> Result<(), ByteBudgetError> {
        let requested = bytes as u64;
        if requested > self.limit {
            return Err(ByteBudgetError::ExceedsBudget {
                requested,
                limit: self.limit,
            });
        }
        if requested > self.available() {
            return Err(ByteBudgetError::Exhausted {
                requested,
                available: self.available(),
                limit: self.limit,
            });
        }
        self.used += requested;
        Ok(())
    }

    /// Returns `bytes` to the budget.
    ///
    /// Saturating rather than panicking on an over-release: an accounting slip
    /// must not take down a node, and the resulting slack is bounded by the
    /// limit itself.
    pub(crate) fn release(&mut self, bytes: usize) {
        self.used = self.used.saturating_sub(bytes as u64);
    }
}

/// Reserves `bytes` against a slot cap and its peer budget together, rolling
/// the slot back if the peer budget refuses.
///
/// The pair has to move as one — a slot reservation that outlives a failed peer
/// reservation leaks budget for the life of the epoch, which is precisely the
/// leak the `live_slots` gauge cannot see.
pub(crate) fn try_reserve_pair(
    peer: &mut ByteBudget,
    slot: &mut ByteBudget,
    bytes: usize,
) -> Result<(), ByteBudgetError> {
    slot.try_reserve(bytes)?;
    match peer.try_reserve(bytes) {
        Ok(()) => Ok(()),
        Err(err) => {
            slot.release(bytes);
            Err(err)
        }
    }
}

/// Returns `bytes` to both scopes.
pub(crate) fn release_pair(peer: &mut ByteBudget, slot: &mut ByteBudget, bytes: usize) {
    slot.release(bytes);
    peer.release(bytes);
}

#[cfg(test)]
mod tests;
