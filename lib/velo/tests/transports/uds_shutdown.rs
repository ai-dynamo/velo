// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for UDS graceful shutdown

#![cfg(unix)]

#[macro_use]
mod common;

use common::{OUTER_TEST_TIMEOUT, UdsShutdownClient, shutdown_scenarios};

transport_shutdown_tests!(uds, UdsShutdownClient);

#[tokio::test]
async fn test_uds_admitted_frames_are_written_across_teardown() {
    tokio::time::timeout(
        OUTER_TEST_TIMEOUT,
        shutdown_scenarios::admitted_frames_are_written_across_teardown::<UdsShutdownClient>(),
    )
    .await
    .expect("admitted_frames_are_written_across_teardown timed out");
}

#[tokio::test]
async fn test_uds_a_frame_admitted_while_dialing_survives_teardown() {
    tokio::time::timeout(
        OUTER_TEST_TIMEOUT,
        shutdown_scenarios::a_frame_admitted_while_dialing_survives_teardown::<UdsShutdownClient>(),
    )
    .await
    .expect("a_frame_admitted_while_dialing_survives_teardown timed out");
}
