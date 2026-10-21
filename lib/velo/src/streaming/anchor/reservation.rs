//! Reservation lifetimes before a producer opens its stream.

use super::*;
use anyhow::Context;
use serde::{Deserialize, Serialize};

pub(crate) const DEFAULT_UNATTACHED_TIMEOUT: Duration = Duration::from_secs(120);
pub(crate) const DEFAULT_PREBIND_TIMEOUT: Duration = Duration::from_secs(180);
/// Most reservations in one `_anchor_renew` message. A wire limit: the handler
/// refuses a larger batch, so a sender and its peer must agree on it.
const RENEW_BATCH: usize = 256;

/// Receiver lifetimes before a producer attaches.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct ReservationConfig {
    /// SPSC anchors wait this long for attachment. Defaults to 120 seconds.
    /// `Duration::MAX` means they never time out.
    pub unattached_timeout: Duration,
    /// The accept window of every messenger-mux bind: a prepared slot waits
    /// this long for its `OpenSlot` or its next renewal, and an ordinary attach
    /// waits this long for its first batch. Defaults to 180 seconds.
    pub prebind_timeout: Duration,
}
impl Default for ReservationConfig {
    fn default() -> Self {
        Self {
            unattached_timeout: DEFAULT_UNATTACHED_TIMEOUT,
            prebind_timeout: DEFAULT_PREBIND_TIMEOUT,
        }
    }
}
impl ReservationConfig {
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.unattached_timeout.is_zero(),
            "unattached timeout must be positive"
        );
        anyhow::ensure!(
            !self.prebind_timeout.is_zero(),
            "prebind timeout must be positive"
        );
        Ok(())
    }
}

/// `now + lifetime`, saturating 30 years out as `tokio::time::sleep` does.
///
/// `Instant + Duration` panics on overflow, and a caller that passes
/// `Duration::MAX` as a timeout means "never".
pub(crate) fn deadline_after(lifetime: Duration) -> tokio::time::Instant {
    const FAR_FUTURE: Duration = Duration::from_secs(86_400 * 365 * 30);
    let now = tokio::time::Instant::now();
    now.checked_add(lifetime)
        .unwrap_or_else(|| now + FAR_FUTURE)
}

/// A handle and, for prepared streams, the generation of its reserved slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AnchorReservation {
    handle: StreamAnchorHandle,
    session: Option<u64>,
}
impl AnchorReservation {
    /// Capture the reservation carried with a request. No attachment occurs.
    pub fn new(
        handle: StreamAnchorHandle,
        ticket: Option<&crate::streaming::control::StreamOpenTicket>,
    ) -> Self {
        Self {
            handle,
            session: ticket.map(|ticket| ticket.routing_session_id),
        }
    }
}

impl AnchorManager {
    /// Renew held reservations, grouped by owner in batches of at most 256.
    /// Attached, cancelled, expired, or replaced reservations are no-ops.
    /// The caller schedules renewal and bounds the call with its own deadline.
    /// Only transport failures return an error; successful delivery does not
    /// prove that a reservation still exists.
    pub async fn renew_reservations(
        &self,
        reservations: &[AnchorReservation],
    ) -> anyhow::Result<()> {
        let mut peers = std::collections::HashMap::<_, Vec<_>>::new();
        for reservation in reservations {
            let worker = reservation.handle.unpack().0;
            if worker == self.worker_id {
                self.renew_reservations_local(std::slice::from_ref(reservation));
            } else {
                peers.entry(worker).or_default().push(*reservation);
            }
        }
        // The messenger `register_handlers` installed, as the attach paths use.
        let Some(messenger) = self.messenger_lock.get().or(self.messenger.as_ref()) else {
            anyhow::ensure!(peers.is_empty(), "remote renewal requires a messenger");
            return Ok(());
        };
        let mut calls = futures::stream::FuturesUnordered::new();
        for (worker, reservations) in peers {
            let messenger = Arc::clone(messenger);
            calls.push(async move {
                for batch in reservations.chunks(RENEW_BATCH) {
                    messenger
                        .typed_unary_streaming::<()>("_anchor_renew")
                        .payload(batch)?
                        .worker(worker)
                        .send()
                        .await
                        .with_context(|| format!("renew reservations on {worker}"))?;
                }
                Ok::<_, anyhow::Error>(())
            });
        }
        use futures::StreamExt;
        let mut failure = None;
        while let Some(result) = calls.next().await {
            if let Err(error) = result {
                failure = Some(error);
            }
        }
        failure.map_or(Ok(()), Err)
    }

    pub(crate) fn renew_reservations_local(&self, reservations: &[AnchorReservation]) {
        for reservation in reservations {
            let (worker, id) = reservation.handle.unpack();
            if worker != self.worker_id {
                continue;
            }
            let Some(mut entry) = self.registry.get_mut(&id) else {
                continue;
            };
            if entry.attachment || entry.cancel_token.is_cancelled() {
                continue;
            }
            match (reservation.session, entry.prebind.as_ref()) {
                (Some(session), Some(bind)) if bind.ticket.routing_session_id == session => {
                    if let Some(mux) = bind.mux.upgrade() {
                        mux.renew_reservation(bind.anchor_id, session);
                    }
                }
                (None, None) => {
                    let now = tokio::time::Instant::now();
                    if let (Some(expires), Some(timeout)) =
                        (entry.expires, entry.unattached_timeout)
                        && expires > now
                    {
                        entry.expires = Some(deadline_after(timeout));
                    }
                }
                _ => {}
            }
        }
    }
}

pub(crate) fn renewal_handler(
    manager: std::sync::Weak<AnchorManager>,
) -> crate::messenger::Handler {
    crate::messenger::Handler::typed_unary_async(
        "_anchor_renew",
        move |cx: crate::messenger::TypedContext<Vec<AnchorReservation>>| {
            let manager = manager.clone();
            async move {
                anyhow::ensure!(
                    cx.input.len() <= RENEW_BATCH,
                    "reservation renewal batch exceeds {RENEW_BATCH} entries"
                );
                if let Some(manager) = manager.upgrade() {
                    manager.renew_reservations_local(&cx.input);
                }
                Ok(())
            }
        },
    )
    .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_and_invalid_lifetimes() {
        let mut config = ReservationConfig::default();
        assert_eq!(config.unattached_timeout, Duration::from_secs(120));
        assert_eq!(config.prebind_timeout, Duration::from_secs(180));
        assert!(config.validate().is_ok());
        config.unattached_timeout = Duration::ZERO;
        assert!(config.validate().is_err());
        config.unattached_timeout = Duration::from_millis(1);
        config.prebind_timeout = Duration::ZERO;
        assert!(config.validate().is_err());
        config.prebind_timeout = Duration::from_micros(500);
        assert!(config.validate().is_ok());
    }
}
