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
use std::time::Instant;

use velo::{InstanceId, PeerInfo};

use super::Shared;
use super::authority::{AbortOutcome, RegistrationPermit};
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
    let request = decode(&shared, presented, request)?;
    // The task owns the transaction: hyper drops this handler's future when
    // the client disconnects, and an abandoned transaction would leave the
    // owner Busy forever.
    let task = tokio::spawn(transact(Arc::clone(&shared), request));
    match task.await {
        Ok(result) => result,
        Err(error) => Err(HubError::internal(format!(
            "registration task failed: {error}"
        ))),
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

/// Holds the permit for the life of the transaction. If the task stops
/// before it finishes (a panicking extension), `Drop` settles the owner
/// so it is not left Busy: before the write it aborts; after it, it fails
/// closed and queues the entry's removal.
struct Transaction<'a> {
    shared: &'a Shared,
    owner: InstanceId,
    permit: Option<RegistrationPermit>,
    written: Option<Incarnation>,
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        let Some(permit) = self.permit.take() else {
            return;
        };
        tracing::error!(instance = %self.owner, "registration stopped before it finished; settling it");
        let authority = &self.shared.authority;
        match self.written {
            None => {
                if let Ok(AbortOutcome::Revoked(prior)) = authority.abort_registration(permit) {
                    let _ = self.shared.jobs.send(Job::Removal(Removal {
                        id: self.owner,
                        incarnation: prior,
                    }));
                }
            }
            Some(incarnation) => {
                let _ = authority.fail_closed_registration(permit, Some(incarnation));
                let _ = self.shared.jobs.send(Job::Retire(Removal {
                    id: self.owner,
                    incarnation,
                }));
            }
        }
    }
}

impl Transaction<'_> {
    fn permit(&self) -> &RegistrationPermit {
        self.permit
            .as_ref()
            .expect("the permit is held until the end")
    }

    /// Fail before the store write: the prior registration is untouched,
    /// unless the store removed it meanwhile; then deliver that removal
    /// before the staged hides are undone, so readers never see the prior
    /// state again.
    fn abort(&mut self, staged: Vec<AnyStaged>, error: HubError) -> HubError {
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
        drop_in_reverse(staged);
        error
    }

    /// Fail after the store write (R4). One path for every such failure.
    async fn fail_after_write(
        &mut self,
        incarnation: Incarnation,
        staged: Vec<AnyStaged>,
        error: HubError,
    ) -> HubError {
        let shared = self.shared;
        let owner = self.owner;
        match shared.store.remove(owner, incarnation).await {
            Ok(()) | Err(StoreError::NotFound(_) | StoreError::Stale { .. }) => {}
            Err(remove_error) => tracing::warn!(
                instance = %owner,
                %incarnation,
                %remove_error,
                "could not remove the failed registration's entry; it stays reserved until it expires"
            ),
        }
        // The new incarnation replaced the prior one in the store, so this
        // removal covers both: the extensions drop whatever the owner had.
        shared.notify_removed(Removal {
            id: owner,
            incarnation,
        });
        drop_in_reverse(staged);
        // The owner is Busy, so no replacement can be registered with velo.
        if let Some(velo) = &shared.velo {
            velo.unregister_peer(owner);
        }
        let reservation = shared.store.current_incarnation(owner);
        let permit = self
            .permit
            .take()
            .expect("the permit is held until the end");
        if let Err(close_error) = shared
            .authority
            .fail_closed_registration(permit, reservation)
        {
            tracing::warn!(instance = %owner, %close_error, "could not fail the registration closed");
        }
        HubError::new(
            ErrorCode::RegisterAgain,
            format!("registration failed after it was recorded; register again: {error}"),
        )
    }
}

fn drop_in_reverse(mut staged: Vec<AnyStaged>) {
    while let Some(value) = staged.pop() {
        drop(value);
    }
}

async fn transact(shared: Arc<Shared>, request: Request) -> Result<RegisterResponse, HubError> {
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

    let permit = shared
        .authority
        .begin_registration(owner, presented.as_ref())?;
    let mut txn = Transaction {
        shared: &shared,
        owner,
        permit: Some(permit),
        written: None,
    };
    let epoch = txn.permit().epoch();
    let previous = txn.permit().previous();

    // Prepare, in start order.
    let mut staged: Vec<AnyStaged> = Vec::with_capacity(shared.extensions.len());
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
                Ok(Ok(value)) => staged.push(value),
                Ok(Err(rejection)) => return Err(txn.abort(staged, rejection.into())),
                Err(_) => {
                    let error = HubError::unavailable(format!(
                        "extension {:?} did not prepare within {:?}",
                        extension.name(),
                        config.step_timeout
                    ));
                    return Err(txn.abort(staged, error));
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
    let written =
        tokio::time::timeout(config.step_timeout, shared.store.put(record, expected)).await;
    let incarnation = match written {
        Ok(Ok(incarnation)) => incarnation,
        Ok(Err(StoreError::Indeterminate(reason))) => match resolve(&shared, owner, op).await {
            Ok(Some(incarnation)) => {
                txn.written = Some(incarnation);
                let error =
                    HubError::unavailable(format!("store write was indeterminate: {reason}"));
                return Err(txn.fail_after_write(incarnation, staged, error).await);
            }
            Ok(None) => {
                let error =
                    HubError::unavailable(format!("store write did not apply: {reason}; retry"));
                return Err(txn.abort(staged, error));
            }
            Err(error) => {
                return Err(fail_unresolved(
                    &mut txn,
                    staged,
                    previous.map(|lease| lease.incarnation),
                    error,
                ));
            }
        },
        Ok(Err(error @ StoreError::Stale { .. })) => {
            let error = HubError::new(
                ErrorCode::RegisterAgain,
                format!("the prior registration changed meanwhile; register again: {error}"),
            );
            return Err(txn.abort(staged, error));
        }
        Ok(Err(error)) => return Err(txn.abort(staged, error.into())),
        Err(_) => {
            // The put future is gone, but a remote write may still land:
            // treat it as indeterminate.
            match resolve(&shared, owner, op).await {
                Ok(Some(incarnation)) => {
                    txn.written = Some(incarnation);
                    let error = HubError::unavailable("store write timed out after it applied");
                    return Err(txn.fail_after_write(incarnation, staged, error).await);
                }
                Ok(None) => {
                    let error = HubError::unavailable(format!(
                        "store write did not complete within {:?}; retry",
                        config.step_timeout
                    ));
                    return Err(txn.abort(staged, error));
                }
                Err(error) => {
                    return Err(fail_unresolved(
                        &mut txn,
                        staged,
                        previous.map(|lease| lease.incarnation),
                        error,
                    ));
                }
            }
        }
    };
    txn.written = Some(incarnation);

    // Everything below fails through R4.
    if let Err(error) = shared.authority.record_write(txn.permit(), incarnation) {
        return Err(txn
            .fail_after_write(incarnation, staged, error.into())
            .await);
    }
    if let Some(velo) = &shared.velo
        && let Err(error) = velo.register_peer(peer.clone())
    {
        let error = HubError::bad_request(format!("velo cannot reach the peer: {error}"));
        return Err(txn.fail_after_write(incarnation, staged, error).await);
    }
    let committed = Committed {
        owner,
        peer: &peer,
        incarnation,
        epoch,
        previous,
        extensions: &raw,
    };
    for (extension, value) in shared.extensions.iter().zip(staged) {
        extension.commit(value, &committed);
    }
    let lease = match shared
        .authority
        .commit_registration(txn.permit(), incarnation)
    {
        Ok(lease) => lease,
        Err(error) => {
            return Err(txn
                .fail_after_write(incarnation, Vec::new(), error.into())
                .await);
        }
    };
    let credential = txn.permit().credential().clone();
    for extension in &shared.extensions {
        extension.on_registered(&committed).await;
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
    })
}

async fn resolve(
    shared: &Shared,
    owner: InstanceId,
    op: OpId,
) -> Result<Option<Incarnation>, StoreError> {
    match tokio::time::timeout(shared.config.step_timeout, shared.store.resolve(owner, op)).await {
        Ok(result) => result,
        Err(_) => Err(StoreError::Backend(anyhow::anyhow!(
            "resolve did not complete within {:?}",
            shared.config.step_timeout
        ))),
    }
}

/// The write's outcome cannot be learned. Neither the prior registration
/// nor the attempted one can be trusted, so drop both: deliver the prior's
/// removal, forget the velo peer, and reserve whatever the store holds
/// until it expires.
fn fail_unresolved(
    txn: &mut Transaction<'_>,
    staged: Vec<AnyStaged>,
    prior: Option<Incarnation>,
    error: StoreError,
) -> HubError {
    let shared = txn.shared;
    let owner = txn.owner;
    if let Some(prior) = prior {
        shared.notify_removed(Removal {
            id: owner,
            incarnation: prior,
        });
    }
    drop_in_reverse(staged);
    if let Some(velo) = &shared.velo {
        velo.unregister_peer(owner);
    }
    let reservation = shared.store.current_incarnation(owner);
    let permit = txn.permit.take().expect("the permit is held until the end");
    let _ = shared
        .authority
        .fail_closed_registration(permit, reservation);
    HubError::unavailable(format!(
        "store write outcome is unknown and could not be resolved ({error}); the registration is dropped and its entry reserved until it expires"
    ))
}

/// `DELETE /v1/instances/{id}`. Takes the registration lock and delivers
/// the removal itself, so the extensions have dropped the owner when it
/// answers. The hook also queues the delivery; that second one is a no-op.
pub(crate) async fn unregister(
    shared: &Shared,
    owner: InstanceId,
    presented: Option<Credential>,
) -> Result<(), HubError> {
    let _guard = tokio::time::timeout(shared.config.lock_timeout, shared.lock.lock())
        .await
        .map_err(|_| HubError::unavailable("timed out waiting for the registration lock"))?;
    let permit = shared
        .authority
        .begin_unregister(owner, presented.as_ref())?;
    let removal = Removal {
        id: owner,
        incarnation: permit.incarnation(),
    };
    match shared.store.remove(owner, removal.incarnation).await {
        Ok(()) => {
            shared.deliver_removal(removal);
            Ok(())
        }
        Err(error @ (StoreError::NotFound(_) | StoreError::Stale { .. })) => {
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
