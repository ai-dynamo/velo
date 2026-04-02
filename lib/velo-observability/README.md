<!--
SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# velo-observability

Shared Prometheus metrics and optional tracing helpers for Velo.

This crate does not run an exporter. Callers create a Prometheus `Registry`,
register `VeloMetrics` into it, and expose or scrape that registry however they
want.

## Usage

```rust
use prometheus::Registry;
use velo_observability::VeloMetrics;

let registry = Registry::new();
let metrics = VeloMetrics::register(&registry)?;
```

Pass the resulting `Arc<VeloMetrics>` into higher-level Velo components such as
`Messenger` or `Velo`.

## Metric Families

- Transport: frame counts, frame bytes, rejections, registered peers, active connections.
- Messenger: handler requests, durations, request/response bytes, in-flight handlers,
  dispatch failures, client resolution, pending responses.
- Streaming: anchor operations, durations, active anchors, backpressure.
- Rendezvous: operations, durations, transferred bytes, active staged slots.

The crate exposes typed enums such as `Direction`, `HandlerOutcome`,
`TransportRejection`, `DispatchFailure`, `StreamingOp`, and `RendezvousOp` so
call sites can avoid stringly-typed labels.

## Tracing

Enable the `distributed-tracing` feature to use the OpenTelemetry propagation
helpers that inject the current context into message headers and apply a remote
parent on receive.

## Test Helpers

The `test_helpers` module provides `MetricSnapshot` and related helpers for
asserting counter, gauge, and histogram values in integration tests.
