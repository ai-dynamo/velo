// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The registration transaction, and unregistration.
//!
//! ```text
//! handler: decode payloads, check registration_requires, admit   (400)
//!          spawn the transaction task and await it; a client that
//!          disconnects cannot abandon it
//! task:    lock (503 on timeout) -> begin (401/409 Busy/503)
//!          prepare each extension, under a deadline
//!            fail: abort -> the prior registration is untouched
//!          put, under a deadline; Indeterminate -> resolve by op id
//!          record_write -> velo register_peer -> commit each extension
//!          -> commit the credential -> on_registered each -> unlock
//!            any failure after put: remove the new entry, on_removed on
//!            every extension, forget the velo peer, fail the credential
//!            closed, answer 409 register_again
//! ```

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use velo::{InstanceId, PeerInfo};

use super::Shared;
use super::authority::{AbortOutcome, AuthorityError, RegistrationPermit};
use super::error::HubError;
use super::extension::{AnyPayload, AnyStaged, Committed, RegistrationView, Txn};
use super::lifecycle::Job;
use crate::protocol::{Credential, ErrorCode, RegisterRequest, RegisterResponse};
use crate::store::{CredentialDigest, Incarnation, OpId, RegistrationRecord, Removal, StoreError};

/// One decoded registration, ready for the transaction task.
struct Request {
    peer: PeerInfo,
    presented: Option<Credential>,
    raw: BTreeMap<String, serde_json::Value>,
    /// One slot per extension, in start order.
    payloads: Vec<Option<AnyPayload>>,
}

/// `POST /v1/instances`.
pub(crate) async fn register(
    shared: Arc<Shared>,
    presented: Option<Credential>,
    request: RegisterRequest,
) -> Result<RegisterResponse, HubError> {
    if !shared.admitting() {
        return Err(HubError::unavailable("the hub is not taking registrations"));
    }
    // Decoding runs in the task too, so a panic in an extension's `decode`
    // or `admit` meets the same boundary as one in `prepare` and answers 500
    // instead of dropping the connection.
    let owner = request.peer.instance_id();
    let checked = presented.clone();
    let authority = shared.authority.clone();
    // Set by the transaction when its store write is recorded: a failure
    // after it dropped the prior registration. Authority state alone cannot
    // say so: a reserved owner (recovery) fails `authorize` either way.
    let written = Arc::new(AtomicBool::new(false));
    let answer = hub_task("registration", {
        let written = Arc::clone(&written);
        async move {
            let request = decode(&shared, presented, request)?;
            transact(shared, request, written).await
        }
    })
    .await;
    // A panic after the write has dropped the prior registration (the
    // transaction's `Drop` failed it closed); say so, as every other
    // post-write failure does, or the client keeps a dead credential.
    // The prior registration is gone if the write happened, or if a removal
    // took it while the transaction ran (the credential is now unknown to
    // the hub). Not on `Recovered` (a reserved owner's credential can still
    // recover it) or `Unavailable`/`Busy` (the hub cannot tell yet).
    let prior_gone = |credential: &Credential| {
        written.load(Ordering::Acquire)
            || matches!(
                authority.authorize(owner, Some(credential)),
                Err(AuthorityError::Unauthorized { .. } | AuthorityError::NotFound { .. })
            )
    };
    match answer {
        Err(error)
            if error.code() == ErrorCode::Internal && checked.as_ref().is_some_and(prior_gone) =>
        {
            Err(HubError::new(
                ErrorCode::RegisterAgain,
                format!("the registration failed after it was recorded; register again: {error}"),
            ))
        }
        other => other,
    }
}

/// Run `work` as a task of the hub and wait for it. hyper drops a handler's
/// future when its client closes the socket; work that holds an owner's
/// permit must outlive that, or the owner stays Busy for ever. A panic in
/// the task answers 500.
async fn hub_task<T: Send + 'static>(
    what: &str,
    work: impl Future<Output = Result<T, HubError>> + Send + 'static,
) -> Result<T, HubError> {
    match tokio::spawn(work).await {
        Ok(result) => result,
        Err(error) => Err(HubError::internal(format!("{what} task failed: {error}"))),
    }
}

fn decode(
    shared: &Shared,
    presented: Option<Credential>,
    request: RegisterRequest,
) -> Result<Request, HubError> {
    let RegisterRequest { peer, extensions } = request;
    if let Some(unknown) = extensions
        .keys()
        .find(|name| !shared.by_name.contains_key(name.as_str()))
    {
        return Err(HubError::bad_request(format!(
            "the hub has no extension {unknown:?}"
        )));
    }
    let mut payloads: Vec<Option<AnyPayload>> = Vec::with_capacity(shared.extensions.len());
    for extension in &shared.extensions {
        let payload = match extensions.get(extension.name()) {
            None => None,
            Some(value) => Some(extension.decode(value.clone()).map_err(|error| {
                HubError::bad_request(format!(
                    "payload for extension {:?}: {error}",
                    extension.name()
                ))
            })?),
        };
        if payload.is_some() {
            for &required in extension.registration_requires() {
                if !extensions.contains_key(required) {
                    return Err(HubError::bad_request(format!(
                        "extension {:?} requires the registrant to also opt into {required:?}",
                        extension.name()
                    )));
                }
            }
        }
        payloads.push(payload);
    }
    // The hub's velo instance must be able to decode the address; without
    // velo the address is opaque and only stored.
    if shared.velo.is_some()
        && let Err(error) = peer.worker_address().available_transports()
    {
        return Err(HubError::bad_request(format!(
            "peer address does not decode: {error}"
        )));
    }
    let view = RegistrationView {
        peer: &peer,
        extensions: &extensions,
    };
    for (extension, payload) in shared.extensions.iter().zip(&payloads) {
        extension.admit(&view, payload.as_ref())?;
    }
    Ok(Request {
        peer,
        presented,
        raw: extensions,
        payloads,
    })
}

/// Holds the permit and the staged values for the life of the
/// transaction. If the task stops before it finishes (a panicking
/// extension), `Drop` settles the owner so it is not left Busy: before the
/// write it aborts; after it, it fails closed and queues the removal of
/// the entry. `Drop` runs before the lock guard is released, so it delivers
/// removals itself, before it drops the staged values: an undo in a staged
/// value must never show state that was just removed.
struct Transaction<'a> {
    /// Fields drop after `Drop::drop` has settled the transaction, so the
    /// heartbeat does not probe the owner until then.
    _in_flight: super::heartbeat::InFlightGuard<'a>,
    shared: &'a Shared,
    owner: InstanceId,
    permit: Option<RegistrationPermit>,
    written: Option<Incarnation>,
    /// One slot per extension that prepared, in start order; `commit`
    /// takes each one.
    staged: Vec<Option<AnyStaged>>,
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        if self.permit.is_none() {
            return;
        }
        tracing::error!(instance = %self.owner, "registration stopped before it finished; settling it");
        let shared = self.shared;
        match self.written {
            None => self.settle_before_write(),
            Some(incarnation) => {
                // Reserve only what the store still holds: a removal can have
                // taken the entry already, and nothing would clear a
                // reservation for an entry that is gone.
                let reservation = shared
                    .store
                    .current_incarnation(self.owner)
                    .filter(|current| *current == incarnation);
                let removal = Removal {
                    id: self.owner,
                    incarnation,
                };
                self.settle_after_write(Some(removal), reservation);
                // The store write needs an await, which Drop cannot do.
                let _ = shared.jobs.send(Job::Retire(removal));
            }
        }
    }
}

impl Transaction<'_> {
    /// Drop the staged values not yet committed, last first.
    fn drop_staged(&mut self) {
        while let Some(value) = self.staged.pop() {
            drop(value);
        }
    }

    /// End a registration that may have written and will not finish. The
    /// one order for every such end: deliver `removal` before the staged
    /// undos run, so readers never see the prior state again; forget the
    /// velo peer (the owner is Busy, so no replacement is registered with
    /// velo); fail the credential closed, reserving `reservation`. Sync, so
    /// `Drop` can call it.
    fn settle_after_write(&mut self, removal: Option<Removal>, reservation: Option<Incarnation>) {
        let shared = self.shared;
        if let Some(removal) = removal {
            shared.notify_removed(removal);
        }
        self.drop_staged();
        if let Some(velo) = &shared.velo {
            velo.unregister_peer(self.owner);
        }
        let permit = self
            .permit
            .take()
            .expect("the permit is held until the end");
        if let Err(close_error) = shared
            .authority
            .fail_closed_registration(permit, reservation)
        {
            tracing::warn!(instance = %self.owner, %close_error, "could not fail the registration closed");
        }
    }

    fn permit(&self) -> &RegistrationPermit {
        self.permit
            .as_ref()
            .expect("the permit is held until the end")
    }

    /// Fail before the store write: the prior registration is untouched,
    /// unless the store removed it meanwhile; then deliver that removal
    /// before the staged hides are undone, so readers never see the prior
    /// state again.
    fn abort(&mut self, error: HubError) -> HubError {
        self.settle_before_write();
        error
    }

    /// The body of [`abort`](Self::abort), which `Drop` also runs.
    fn settle_before_write(&mut self) {
        let permit = self
            .permit
            .take()
            .expect("the permit is held until the end");
        match self.shared.authority.abort_registration(permit) {
            Ok(AbortOutcome::Restored) => {}
            Ok(AbortOutcome::Revoked(prior)) => self.shared.deliver_removal(Removal {
                id: self.owner,
                incarnation: prior,
            }),
            Err(abort_error) => {
                tracing::warn!(instance = %self.owner, %abort_error, "could not abort registration");
            }
        }
        self.drop_staged();
    }

    /// The write's outcome cannot be learned. Neither the prior registration
    /// nor the attempted one can be trusted, so drop both: deliver the prior's
    /// removal, forget the velo peer, and reserve whatever the store holds
    /// until it expires.
    fn fail_unresolved(&mut self, prior: Option<Incarnation>, error: StoreError) -> HubError {
        let removal = prior.map(|incarnation| Removal {
            id: self.owner,
            incarnation,
        });
        let reservation = self.shared.store.current_incarnation(self.owner);
        self.settle_after_write(removal, reservation);
        // Not `unavailable`: the prior registration is gone, and a client
        // that read "unavailable" as "nothing happened" would keep its dead
        // credential. When a credential was presented and an entry is
        // reserved, that credential recovers it (the reserved record is
        // either the attempted write, which names it as presented, or the
        // prior registration, which minted it): say `recovered`, so the
        // client keeps it and registers again with it.
        let code = if prior.is_some() && reservation.is_some() {
            ErrorCode::Recovered
        } else {
            ErrorCode::RegisterAgain
        };
        HubError::new(
            code,
            format!(
                "store write outcome is unknown and could not be resolved ({error}); the registration is dropped and its entry reserved"
            ),
        )
    }

    /// Fail after the store write. One path for every such failure: remove
    /// the new entry, deliver its removal, forget the velo peer, and fail
    /// the credential closed.
    async fn fail_after_write(&mut self, incarnation: Incarnation, error: HubError) -> HubError {
        let shared = self.shared;
        let owner = self.owner;
        if let Err(remove_error) = shared.remove_bounded(owner, incarnation).await
            && !remove_error.is_gone()
        {
            tracing::warn!(
                instance = %owner,
                %incarnation,
                %remove_error,
                "could not remove the failed registration's entry; it stays reserved until it expires"
            );
        }
        // The new incarnation replaced the prior one in the store, so this
        // removal covers both: the extensions drop whatever the owner had.
        let removal = Removal {
            id: owner,
            incarnation,
        };
        self.settle_after_write(Some(removal), shared.store.current_incarnation(owner));
        HubError::new(
            ErrorCode::RegisterAgain,
            format!("registration failed after it was recorded; register again: {error}"),
        )
    }
}

async fn transact(
    shared: Arc<Shared>,
    request: Request,
    written: Arc<AtomicBool>,
) -> Result<RegisterResponse, HubError> {
    let Request {
        peer,
        presented,
        raw,
        payloads,
    } = request;
    let owner = peer.instance_id();
    let config = &shared.config;

    let _guard = tokio::time::timeout(config.lock_timeout, shared.lock.lock())
        .await
        .map_err(|_| HubError::unavailable("timed out waiting for the registration lock"))?;
    if !shared.admitting() {
        return Err(HubError::unavailable("the hub is not taking registrations"));
    }
    let locked_at = Instant::now();

    // Read under the lock: the record a reserved owner recovers from.
    let stored = presented.as_ref().and_then(|_| shared.store.get(owner));
    let permit = shared
        .authority
        .begin_registration(owner, presented.as_ref(), stored.as_ref())?;
    let mut txn = Transaction {
        _in_flight: shared.in_flight.enter(owner),
        shared: &shared,
        owner,
        permit: Some(permit),
        written: None,
        staged: Vec::with_capacity(shared.extensions.len()),
    };
    let epoch = txn.permit().epoch();
    let previous = txn.permit().previous();

    // Prepare, in start order.
    {
        let view = Txn {
            owner,
            peer: &peer,
            epoch,
            previous,
            extensions: &raw,
        };
        for (extension, payload) in shared.extensions.iter().zip(payloads) {
            let prepared =
                tokio::time::timeout(config.step_timeout, extension.prepare(&view, payload)).await;
            match prepared {
                Ok(Ok(value)) => txn.staged.push(Some(value)),
                Ok(Err(rejection)) => return Err(txn.abort(rejection.into())),
                Err(_) => {
                    let error = HubError::unavailable(format!(
                        "extension {:?} did not prepare within {:?}",
                        extension.name(),
                        config.step_timeout
                    ));
                    return Err(txn.abort(error));
                }
            }
        }
    }

    // Write.
    let record = RegistrationRecord {
        peer: peer.clone(),
        op: OpId::new(),
        epoch: Some(epoch),
        accepted: presented.as_ref().map(CredentialDigest::of),
        pending: Some(CredentialDigest::of(txn.permit().credential())),
        extensions: raw.clone(),
        pinned: false,
    };
    let op = record.op;
    let expected = previous.map(|lease| lease.incarnation);
    // A put that times out is indeterminate like one the backend reports
    // as such: a remote write may still land after its future is dropped.
    let incarnation = match super::bounded(config.step_timeout, shared.store.put(record, expected))
        .await
    {
        Ok(incarnation) => incarnation,
        Err(StoreError::Indeterminate(reason)) => {
            match super::bounded(config.step_timeout, shared.store.resolve(owner, op)).await {
                Ok(Some(incarnation)) => {
                    txn.written = Some(incarnation);
                    written.store(true, Ordering::Release);
                    let error =
                        HubError::unavailable(format!("store write was indeterminate: {reason}"));
                    return Err(txn.fail_after_write(incarnation, error).await);
                }
                Ok(None) => {
                    let error = HubError::unavailable(format!(
                        "store write did not apply: {reason}; retry"
                    ));
                    return Err(txn.abort(error));
                }
                Err(error) => {
                    return Err(txn.fail_unresolved(previous.map(|lease| lease.incarnation), error));
                }
            }
        }
        Err(error @ StoreError::Stale { .. }) => {
            let error = HubError::new(
                ErrorCode::RegisterAgain,
                format!("the prior registration changed meanwhile; register again: {error}"),
            );
            return Err(txn.abort(error));
        }
        Err(error) => return Err(txn.abort(error.into())),
    };
    txn.written = Some(incarnation);
    written.store(true, Ordering::Release);

    // Everything below fails through `fail_after_write`.
    if let Err(error) = shared.authority.record_write(txn.permit(), incarnation) {
        return Err(txn.fail_after_write(incarnation, error.into()).await);
    }
    if let Some(velo) = &shared.velo
        && let Err(error) = velo.register_peer(peer.clone())
    {
        let error = HubError::bad_request(format!("velo cannot reach the peer: {error}"));
        return Err(txn.fail_after_write(incarnation, error).await);
    }
    let committed = Committed {
        owner,
        peer: &peer,
        incarnation,
        epoch,
        previous,
        extensions: &raw,
    };
    // Each slot is taken just before its commit, so if a commit panics the
    // values not yet committed are still in the transaction, and its Drop
    // delivers the removal before it drops them. The panicking extension's
    // own value is in its commit frame and drops during the unwind, first;
    // the Extension rules say so.
    for (index, extension) in shared.extensions.iter().enumerate() {
        let value = txn.staged[index]
            .take()
            .expect("every extension prepared before the write");
        extension.commit(value, &committed);
    }
    let lease = match shared
        .authority
        .commit_registration(txn.permit(), incarnation)
    {
        Ok(lease) => lease,
        Err(error) => {
            return Err(txn.fail_after_write(incarnation, error.into()).await);
        }
    };
    let credential = txn.permit().credential().clone();
    for extension in &shared.extensions {
        // The contract says on_registered must not block; the deadline
        // keeps one that does from holding the lock.
        if tokio::time::timeout(config.step_timeout, extension.on_registered(&committed))
            .await
            .is_err()
        {
            tracing::error!(
                extension = extension.name(),
                instance = %owner,
                "on_registered did not return within {:?}; it must spawn slow work",
                config.step_timeout
            );
        }
    }
    // Disarm only now: if an on_registered panics, Drop retires the entry,
    // whose removal then revokes the credential that was just committed.
    txn.permit = None;
    tracing::debug!(
        instance = %owner,
        %incarnation,
        held_us = locked_at.elapsed().as_micros() as u64,
        "registered"
    );
    Ok(RegisterResponse {
        instance_id: owner,
        hub_instance_id: shared.velo.as_ref().map(|velo| velo.instance_id()),
        credential,
        epoch: lease.epoch,
        hub_id: shared.hub_id,
    })
}

/// `DELETE /v1/instances/{id}`. Takes the registration lock and delivers
/// the removal itself, so the extensions have dropped the owner when it
/// answers. The hook also queues the delivery; that second one is a no-op.
pub(crate) async fn unregister(
    shared: Arc<Shared>,
    owner: InstanceId,
    presented: Option<Credential>,
) -> Result<(), HubError> {
    hub_task("unregistration", async move {
        unregister_now(&shared, owner, presented).await
    })
    .await
}

async fn unregister_now(
    shared: &Shared,
    owner: InstanceId,
    presented: Option<Credential>,
) -> Result<(), HubError> {
    let _guard = tokio::time::timeout(shared.config.lock_timeout, shared.lock.lock())
        .await
        .map_err(|_| HubError::unavailable("timed out waiting for the registration lock"))?;
    if !shared.admitting() {
        return Err(HubError::unavailable("the hub is stopping"));
    }
    let permit = shared
        .authority
        .begin_unregister(owner, presented.as_ref())?;
    let removal = Removal {
        id: owner,
        incarnation: permit.incarnation(),
    };
    match shared.remove_bounded(owner, removal.incarnation).await {
        Ok(()) => {
            shared.deliver_removal(removal);
            Ok(())
        }
        Err(error) if error.is_gone() => {
            // Gone already (expired meanwhile): make sure its removal is
            // delivered, then report what the store said.
            shared.authority.begin_revocation(removal);
            shared.deliver_removal(removal);
            Err(error.into())
        }
        Err(StoreError::Indeterminate(reason)) => {
            if shared.store.is_current(owner, removal.incarnation) {
                shared.authority.restore_unregister(permit);
                Err(HubError::unavailable(format!(
                    "store remove did not apply: {reason}; retry"
                )))
            } else {
                shared.authority.begin_revocation(removal);
                shared.deliver_removal(removal);
                Ok(())
            }
        }
        Err(error) => {
            shared.authority.restore_unregister(permit);
            Err(error.into())
        }
    }
}
