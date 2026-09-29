// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! QUIC messenger transport (feature `quic`).
//!
//! One QUIC connection per peer and direction, like TCP. The dialer opens one
//! bidirectional stream and writes velo frames on it with the shared
//! coalescing writer. The listener reads the stream with the TCP frame codec
//! and writes back only `ShuttingDown` echoes. One stream per peer keeps the
//! order of messages from one peer, which ordered handlers and batched
//! streaming rely on.
//!
//! With `QuicTransportBuilder::lanes(n)`, the dialer keeps `n` connections to
//! each peer, one per lane, each from its own UDP socket. `send_message` uses
//! lane 0, so ordinary traffic keeps one ordered channel per peer. A caller
//! that sends on other lanes gets order within each lane only. One connection
//! is bound to about one core, so lanes are how one peer gets more than about
//! 0.8 GB/s.
//!
//! TLS 1.3 uses a self-signed certificate per transport. Its SHA-256
//! fingerprint travels in the `WorkerAddress` entry, and dialers accept only
//! that certificate.

mod endpoint;
mod listener;
// Public, and with quinn's types in its API, only under `test-helpers`: the
// integration tests dial a transport with a raw quinn client. Hidden from the
// docs because it is not an API to depend on.
#[cfg(feature = "test-helpers")]
#[doc(hidden)]
pub mod tls;
#[cfg(not(feature = "test-helpers"))]
pub(crate) mod tls;
mod transport;

pub use endpoint::QuicEndpointInfo;
pub use transport::{QuicTransport, QuicTransportBuilder};
