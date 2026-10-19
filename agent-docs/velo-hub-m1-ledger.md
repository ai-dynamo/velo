# velo-hub M1: ported-assertion ledger

Work branch only; `agent-docs/` never merges to `main`. 2026-10-18.

Every kvbm-hub test that M1 touches is listed with where it went. Ported tests are classified per assertion as **kept** (same observable behavior, at most a path or name change) or **changed** (with the reason). Source: `kvbm/crates/kvbm-hub` at the revision read on 2026-10-18.

Name and path changes that apply to every row and are not repeated: `/health` -> `/v1/health`, `/v1/config` -> `/v1/hub`, discovery port -> public listener, header `x-kvbm-mutation-credential` -> `x-velo-hub-credential`, `MutationCredential` -> `Credential`, `RegistryIncarnation` -> `Incarnation`, `RegistryRemoval` -> `Removal`, `PeerRegistry` -> `HubStore`, `RegisterRequest { peer_info, features, runtime }` -> `{ peer, extensions }`, `RegisterResponse.mutation_credential/registration_epoch` (optional) -> `credential/epoch` (required), heartbeat handler `kvbm_hub_heartbeat` -> `velo_hub_heartbeat`.

## Counts

| kvbm source | tests | ported to M1 | merged | deferred to M2 (client) | deferred to K1 (kvbm-owned) |
|---|---|---|---|---|---|
| `tests/integration.rs` | 59 | 33 (1 rewritten) | 5 into `protocol::tests` | 13 | 8 |
| `src/registry/tests.rs` | 17 | 17 | | | |
| `src/registration/tests.rs` | 6 | 6 | | | |
| `tests/registration_lifecycle.rs` | 14 | 14 | | | |
| `tests/heartbeat_integration.rs` | 5 | 5 | | | |
| `src/server/heartbeat.rs` (unit) | 2 | 2 | | | |
| `src/server.rs` (unit) | 4 | 2 covered | 2 not applicable | | |
| `tests/client_unregister.rs` | 2 | | | 2 | |

The plan's counts (§11: 45/12/6/8; §13.4: 54/16/6/14) were both off; the table above is from `grep -c '#\[tokio::test\]\|#\[test\]'`. More importantly, "port `tests/integration.rs` with path changes only" was wrong: 21 of its 59 tests exercise kvbm's `HubClient` or kvbm's own features, not the hub.

## `tests/integration.rs` (59) -> `lib/velo-hub/tests/integration.rs`

| # | kvbm test | Destination | Classification |
|---|---|---|---|
| 1 | `heartbeat_handler_name_is_stable` | `protocol::tests::handler_name_and_header_are_stable` | kept (name changed to `velo_hub_heartbeat`) |
| 2 | `heartbeat_request_default` | `protocol::tests::heartbeat_messages_round_trip` | kept |
| 3 | `heartbeat_ack_default` | same | kept |
| 4 | `heartbeat_request_serde_round_trip` | same | kept |
| 5 | `heartbeat_ack_serde_round_trip` | same | kept |
| 6 | `create_heartbeat_handler_builds` | M2 | builds kvbm's client-side handler |
| 7 | `health_check_discovery_port` | same name | kept |
| 8 | `health_check_control_port` | same name | kept |
| 9 | `register_success` | same name | kept; `mutation_credential.is_some()` is now a required field |
| 10 | `active_reregister_requires_current_credential_and_rotates_it` | same name | kept (401, rotate, stale 401) |
| 11 | `unregister_success` | same name | kept |
| 12 | `one_registration_cannot_unregister_another_owner` | same name | kept |
| 13 | `unregister_not_found` | same name | kept |
| 14 | `heartbeat_with_current_credential_is_acknowledged` | same name | kept |
| 15 | `heartbeat_without_credential_is_unauthorized` | same name | kept |
| 16 | `stale_and_cross_owner_credentials_cannot_heartbeat` | same name | kept |
| 17 | `unauthorized_heartbeats_do_not_postpone_expiry` | same name | kept (injected `InMemoryStore` with 1s TTL) |
| 18 | `heartbeat_unregistered_instance` | same name | kept |
| 19 | `get_peer_by_instance_found` | same name | kept |
| 20 | `get_peer_by_instance_not_found` | same name | kept |
| 21 | `get_peer_by_worker_found` | same name | kept |
| 22 | `get_peer_by_worker_not_found` | same name | kept |
| 23 | `control_port_mirrors_discovery_endpoints` | same name | kept |
| 24 | `peers_snapshot_tracks_registrations` | same name | kept (`hub.store().list()` for `state().peers()`) |
| 25 | `peers_snapshot_tracks_unregistrations` | same name | kept |
| 26 | `client_builder_requires_host` | M2 | HubClient |
| 27 | `client_starts_unregistered` | M2 | HubClient |
| 28 | `client_register_sets_is_registered` | M2 | HubClient |
| 29 | `client_register_twice_errors` | M2 | HubClient |
| 30 | `client_heartbeat_while_registered` | M2 | HubClient |
| 31 | `client_heartbeat_before_register_errors` | M2 | HubClient |
| 32 | `client_discover_by_instance_id` | M2 | HubClient |
| 33 | `client_discover_by_worker_id` | M2 | HubClient |
| 34 | `client_unregister_removes_from_server` | M2 | HubClient |
| 35 | `client_unregister_noop_when_not_registered` | M2 | HubClient |
| 36 | `client_discover_after_unregister_errors` | M2 | HubClient |
| 37 | `registration_guard_drop_fires_delete` | M2 | the client's drop guard |
| 38 | `registered_client_visible_in_list_then_removed_on_drop` | `registered_instance_visible_in_list_then_removed` | **changed**: the guard drop that sent DELETE is client work (M2); the DELETE is explicit. The list assertions (empty, one entry with that id, empty) are kept. |
| 39 | `velo_probe_happy_path` | same name | kept; registration and the mutual peer wiring are raw HTTP, not HubClient |
| 40 | `velo_probe_after_client_velo_shutdown_returns_bad_gateway` | same name | kept (502); also asserts code `bad_gateway` |
| 41 | `register_response_includes_hub_instance_id_when_velo_configured` | same name | kept; lookup by raw HTTP |
| 42 | `register_response_hub_instance_id_none_without_transport` | same name | kept |
| 43 | `hub_self_registered_in_registry` | same name | kept; also asserts the entry is pinned |
| 44 | `hub_self_entry_survives_reaper` | same name | kept (`protect` -> pinned record) |
| 45 | `feature_register_without_manager_rejects` | `payload_for_unknown_extension_is_rejected_and_writes_nothing` | kept: 400 `bad_request`, then 404 for the peer. kvbm's rollback after a write is replaced by rejection before any write. |
| 46 | `failed_authorized_reregister_restores_prior_registration` | same name, `Role` toy for CD | kept (400; store still holds it; prefill list unchanged, decode empty; DELETE with the original credential 204) |
| 47 | `feature_cd_list_empty_on_both_ports` | `extension_list_empty_on_both_ports` | kept (`/v1/features/disagg/instances` -> `/v1/ext/role/instances`) |
| 48 | `register_without_features_field_still_works` | `register_without_extensions_field_still_works` | kept |
| 49 | `feature_cd_role_conflict_on_reregister` | `role_conflict_on_reregister` | kept (400) |
| 50 | `feature_cd_unregister_removes_from_lists` | `unregister_removes_from_extension_lists` | kept: the list is empty when DELETE returns, because DELETE takes the lock and delivers the removal itself |
| 51 | `feature_cd_reaper_evicts_from_lists` | `reaper_eviction_reaches_extensions_and_clears_the_credential` | kept, with one tolerance: delivery is now asynchronous (the lifecycle task), so the test polls for the list to empty and allows a short Busy window before the fresh register answers 200 |
| 52 | `feature_cd_prefill_and_decode_register_and_list` | K1 | needs kvbm's CD client and P2P layout payloads |
| 53 | `dispatcher_worker_drains_queue_and_invokes_dispatcher` | K1 | kvbm's CD dispatcher |
| 54 | `no_dispatcher_does_not_spawn_worker` | K1 | kvbm's CD dispatcher |
| 55 | `indexer_find_blocks_velo_lookup` | K1 | kvbm indexer |
| 56 | `indexer_lookup_client_none_when_indexer_disabled` | K1 | kvbm indexer client |
| 57 | `indexer_lookup_client_errs_when_hub_has_no_velo` | K1 | kvbm indexer client |
| 58 | `indexer_client_pushes_a_tier_placement_snapshot` | K1 | kvbm tier placement |
| 59 | `tier_placement_snapshot_push_rejects_wrong_epoch_and_foreign_instance` | K1 | kvbm tier placement |

Added beside them: `public_port_does_not_accept_registrations` (the control/public split kvbm asserted in `control_routes.rs`).

## `src/registry/tests.rs` (17) -> `lib/velo-hub/src/store/memory/tests.rs`

All kept. One change applies to all: kvbm's `register(peer)` was unconditional; `put(record, expected)` is compare-and-swap, so each test passes the incarnation it expects (`None` for the first write). The conformance suite adds the new rule itself (`put_is_conditional`).

| kvbm test | Classification |
|---|---|
| `register_and_list` | kept |
| `reregister_same_instance_replaces_single_entry` | kept |
| `reregister_mints_incarnation_and_rejects_stale_mutations` | kept (`StaleIncarnation` -> `Stale`) |
| `reregister_purges_every_worker_mapping_owned_by_instance` | **intentionally removed** (mega review round 1): it planted a worker claim that no public API can create (`WorkerId` is a function of `InstanceId`), and the scan it defended cost 21 µs per write at 10,000 entries. `write_cost` records the numbers. |
| `removal_callback_identifies_removed_incarnation` | kept |
| `hook_installation_returns_existing_registrations` | kept |
| `removal_hook_cannot_be_replaced` | kept (`HookInstalled`) |
| `hook_snapshot_and_removal_share_a_linearization_point` | kept (64 races) |
| `registrations_snapshot_pairs_peer_with_incarnation` | kept (`list()` now returns records with incarnations) |
| `conflict_on_different_instance_same_worker` | kept |
| `unregister_removes_from_all_maps` | kept |
| `unregister_not_found` | kept |
| `touch_not_found` | kept |
| `prune_stale_removes_old_entries` | kept |
| `prune_reports_the_expired_incarnation` | kept |
| `touch_refreshes_ttl` | kept |
| `protect_exempts_from_prune` | `pinned_entry_is_exempt_from_prune`: kept; pinning is a record field, part of the trait |

## `src/registration/tests.rs` (6) -> `lib/velo-hub/src/hub/authority/tests.rs`

kvbm ran manager cleanup inside the removal callback and counted `on_unregister` calls. The state machine is ported unchanged, but delivery moved to the lifecycle task, so the tests assert what the state machine tells the caller (`RevocationAction`, `AbortOutcome`) instead of a manager counter.

| kvbm test | Classification |
|---|---|
| `reserved_registry_occupant_cannot_begin_client_registration` | kept |
| `removal_before_startup_snapshot_sync_does_not_leave_phantom_reservation` | kept |
| `stale_removal_does_not_revoke_replacement_registration` | kept; "manager.unregister_count() == 0" becomes "begin_revocation answers Ignore" |
| `removal_of_attempted_incarnation_prevents_registration_commit` | commit fails `StateChanged`: kept. **Changed**: kvbm then aborted (Revoked, cleanup once); under B a failure after the write fails closed, so the test calls `fail_closed_registration` and asserts the owner registers afresh |
| `fail_closed_rollback_reserves_a_registry_entry_that_could_not_be_removed` | Unauthorized after fail-closed with a reservation: kept. The cleanup count moves to the transaction (`hooks::post_write_*`) |
| `replacement_is_busy_until_matching_removal_cleanup_finishes` | kept: Busy between `begin_revocation` (Cleanup) and `finish_revocation`; kvbm blocked a thread inside cleanup to hold the window open, here the window is the two calls |

New beside them: `abort_restores_the_prior_registration_unless_it_was_removed`, `authorize_is_busy_mid_registration`, `authority_fails_closed_while_the_store_is_not_live`, `authorize_distinguishes_unknown_from_unauthorized`. The `Unavailable` error for a poisoned mutex is gone (`parking_lot` does not poison); `Unavailable` now means "store not live".

## `tests/registration_lifecycle.rs` (14) -> `lib/velo-hub/tests/registration_lifecycle.rs`

kvbm's `CustomRegistry` -> `FaultStore` (which passes the conformance suite with no fault armed). kvbm's `IndexerManager` and its velo bundle handlers -> the `Ledger` toy extension and its HTTP routes (publish, invalidate: control listener with `Authorized`; query: public listener). The Ledger applies R1 (prepare hides and closes the publish path, owned by `Staged`) and R2 (publish fenced by lease epoch under its own lock), which is what the plan says kvbm's indexer becomes in K1.

| kvbm test (line) | Assertion | Classification |
|---|---|---|
| `registry_write_failure_restores_prior_registration_credential` (:291) | failed put answers 500 | kept |
| | store still holds the instance | kept |
| | DELETE with the prior credential 204 | kept |
| `custom_registry_http_removal_revokes_registration_authority` (:318) | DELETE 204, fresh register 200 | kept |
| `authenticated_heartbeat_cannot_touch_a_replacement_incarnation` (:343) | gated heartbeat with the stale credential answers 409 after a re-registration | kept |
| | DELETE with the new credential 204 | kept |
| `preloaded_registry_entry_cannot_be_claimed_without_credential` (:391) | register 401 | kept |
| | the preloaded peer is untouched | kept |
| `hub_self_entry_cannot_be_claimed_as_a_client_registration` (:415) | register 401 | kept |
| | the store holds only the hub | kept |
| `failed_server_build_does_not_consume_custom_registry_removal_hook` (:437) | occupied port fails startup | kept (port occupied is now the public listener) |
| | no hub entry left | kept (trivially: listeners bind before velo and the pinned entry) |
| | a second hub on the store hears an expiry; fresh register 200 | kept, with the short Busy-window retry (delivery is asynchronous) |
| `failed_manager_attach_cancels_spawned_work_and_leaves_no_hub_entry` (:476) | attach failure fails startup | kept |
| | work hung off the cancellation token stops | kept (`AttachContext::cancellation`) |
| | no hub entry left; the store is reusable | kept |
| `explicit_shutdown_removes_the_hub_from_a_custom_registry` (:509) | | kept |
| `dropping_the_hub_removes_it_from_a_custom_registry` (:525) | | kept |
| `indexer_omission_revokes_old_authority_before_registry_write` (:703) | replacement registration 200 | kept |
| | stale publish fails while the write is pending | kept (authority answers Busy; the Ledger would also refuse: publish path closed) |
| | stale invalidate fails while the write is pending | kept |
| | query misses while an omission is staged | kept (R1: the only assertion that needs prepare to hide; a mutation that drops the hide fails it) |
| `indexer_omission_stays_revoked_between_global_commit_and_finalization` (:786) | the four assertions above, with `on_registered` gated instead of the put | kept |
| | the omitted authority does not return after finalization | kept |
| `custom_registry_expiry_revokes_indexer_bundle_authority` (:880) | invalid replacement address answers 500 | **changed: 400.** The hub decodes the address before the lock when it runs velo; kvbm wrote, failed in `register_peer`, and replayed |
| | the prior credential still publishes | kept |
| | expiry revokes publish authority | kept |
| | expiry revokes registration authority (fresh register 200) | kept, with the Busy-window retry |
| `failed_indexer_omission_restores_live_tombstone_credential_and_epoch` (:962) | invalid address answers 500 | **changed: 400**, same reason |
| | `restored_incarnation != initial_incarnation` | **changed: equal.** That inequality was an artifact of replay (the rollback was a new write). Under B nothing is written |
| | prior peer metadata kept | kept |
| | heartbeat with the prior credential 200 | kept |
| | live entry generation 9 with the prior epoch | kept |
| | retired generation cannot be republished | kept |
| | live entry still generation 9 | kept |
| `successful_reregistration_starts_a_fresh_bundle_generation_domain` (:1075) | epochs differ; incarnation changes | kept |
| | the replacement does not inherit entries | kept |
| | the replacement restarts at generation one, with its own epoch | kept |

## `tests/heartbeat_integration.rs` (5) -> `lib/velo-hub/tests/heartbeat.rs`

All kept. kvbm registered with `HubClient` and had to hold it so its drop guard would not unregister; here registration is a raw POST plus the mutual peer wiring. `discovery_only_hub_does_not_probe` now also registers a peer and checks nothing probes it away. `hub_does_not_probe_self` skips pinned entries instead of comparing to the hub's id.

## `src/server/heartbeat.rs` unit (2) -> `lib/velo-hub/src/hub/heartbeat.rs`

`stale_probe_failure_cannot_unregister_replacement_incarnation`, `probe_failure_counters_are_scoped_to_incarnation`: kept.

## `src/server.rs` unit (4)

`builder_binds_os_assigned_ports` and `server_entry_point_builder`: covered by every suite (all hubs bind port 0) and `lifecycle::shutdown_frees_ports_and_shuts_velo_down`. `hub_server_state_starts_empty` and `hub_server_state_default_starts_empty`: not applicable; there is no public state type to construct without serving.

## New tests (plan §11 M1 row and §13.4)

| Plan item | Test |
|---|---|
| topo attach/start, reverse shutdown | `lifecycle::attach_and_start_follow_dependencies_and_shutdown_reverses` (also prepare/commit order, reverse `on_removed`, `GET /v1/hub`) |
| cycle/missing/duplicate/unclaimed config rejected before bind | `lifecycle::bad_graphs_are_rejected_before_binding` (public port occupied, so a `GraphError` proves the order), `lifecycle::bundle_graph_errors` |
| unknown payload 400 | `integration::payload_for_unknown_extension_is_rejected_and_writes_nothing`, `hooks::malformed_payload_is_a_bad_request`, `hooks::registration_requires_and_admit_reject_before_anything_is_written` |
| extension cannot shadow core routes | `lifecycle::an_extension_cannot_shadow_core_routes` (+ invalid name in the graph test) |
| prepare failure leaves prior untouched | `hooks::prepare_failure_leaves_prior_untouched`, `hooks::panicking_prepare_does_not_leave_the_owner_busy` |
| post-write failure fails closed and delivers `on_removed` | `hooks::post_write_register_peer_failure_fails_closed_and_leaves_no_velo_peer`, `hooks::indeterminate_put_that_applied_fails_closed`, `hooks::panicking_commit_fails_closed` |
| removal during prepare cannot interleave with commit | `hooks::removal_during_prepare_cannot_interleave_with_commit` (mutation-checked) |
| authorize is Busy mid-registration | `hooks::authorize_is_busy_mid_registration` |
| shutdown frees ports and shuts down Velo | `lifecycle::shutdown_frees_ports_and_shuts_velo_down` (the velo TCP listener's port is rebindable) |
| client disconnect does not leave the owner Busy | `hooks::client_disconnect_mid_transaction_does_not_leave_the_owner_busy` |
| prepare deadline aborts and releases the lock | `hooks::prepare_deadline_aborts_and_releases_the_lock` |
| Indeterminate put resolved both ways | `hooks::indeterminate_put_that_did_not_apply_aborts_and_leaves_prior_untouched`, `hooks::indeterminate_put_that_applied_fails_closed`, plus `..._cannot_be_resolved_drops_and_reserves` |
| post-write `register_peer` failure leaves no velo peer | `hooks::post_write_register_peer_failure_fails_closed_and_leaves_no_velo_peer` |
| delayed removal does not unregister a replacement | `hooks::delayed_removal_of_an_old_incarnation_does_not_unregister_the_replacement` (mutation-checked) |
| continuity Lost closes authorization | `hooks::continuity_lost_closes_authorization_and_keeps_discovery` |
| startup failure after `start` unwinds, store reusable | `lifecycle::startup_failure_after_start_unwinds_and_leaves_the_store_reusable` |
| shutdown waits for an in-flight registration | `lifecycle::shutdown_waits_for_an_in_flight_registration` |
| (R2) a lease cannot write across a re-registration | `hooks::a_lease_taken_before_a_reregistration_cannot_write_after_it` |
| store contract | `store::conformance::run_all` against `InMemoryStore` and `FaultStore` |
