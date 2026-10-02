// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Bounded command queue with an explicit receiver-close boundary.
//!
//! Closing refuses new submissions. Draining also waits for a sender which
//! reserved capacity before close, so accepted commands cannot miss teardown.

use tokio::sync::mpsc;

use super::worker::Cmd;

#[derive(Clone)]
pub(crate) struct CommandSender(mpsc::Sender<Cmd>);

pub(crate) struct CommandReceiver(mpsc::Receiver<Cmd>);

pub(crate) fn command_ring(capacity: usize) -> (CommandSender, CommandReceiver) {
    let (tx, rx) = mpsc::channel(capacity);
    (CommandSender(tx), CommandReceiver(rx))
}

impl CommandSender {
    // Return the command for retry without allocating on the admission path.
    #[allow(clippy::result_large_err)]
    pub fn try_send(&self, cmd: Cmd) -> Result<(), flume::TrySendError<Cmd>> {
        self.0.try_send(cmd).map_err(|error| match error {
            mpsc::error::TrySendError::Full(cmd) => flume::TrySendError::Full(cmd),
            mpsc::error::TrySendError::Closed(cmd) => flume::TrySendError::Disconnected(cmd),
        })
    }

    pub async fn send_async(&self, cmd: Cmd) -> Result<(), flume::SendError<Cmd>> {
        self.0
            .send(cmd)
            .await
            .map_err(|error| flume::SendError(error.0))
    }
}

impl CommandReceiver {
    pub fn try_recv(&mut self) -> Result<Cmd, flume::TryRecvError> {
        self.0.try_recv().map_err(|error| match error {
            mpsc::error::TryRecvError::Empty => flume::TryRecvError::Empty,
            mpsc::error::TryRecvError::Disconnected => flume::TryRecvError::Disconnected,
        })
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn close(&mut self) {
        self.0.close();
    }

    pub fn blocking_recv(&mut self) -> Option<Cmd> {
        self.0.blocking_recv()
    }

    #[cfg(test)]
    pub async fn recv_async(&mut self) -> Result<Cmd, flume::RecvError> {
        self.0.recv().await.ok_or(flume::RecvError::Disconnected)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn close_refuses_waiting_sends_and_drains_reserved_commands() {
        let (tx, mut rx) = command_ring(1);
        tx.try_send(Cmd::Shutdown).unwrap();
        let waiting = tx.send_async(Cmd::Shutdown);
        tokio::pin!(waiting);
        assert!(futures::poll!(&mut waiting).is_pending());
        rx.close();
        assert!(waiting.await.is_err());
        assert!(rx.recv_async().await.is_ok());
        assert!(rx.recv_async().await.is_err());
        assert!(tx.try_send(Cmd::Shutdown).is_err());

        let (tx, mut rx) = command_ring(1);
        let reserved = tx.0.reserve().await.unwrap();
        rx.close();
        let receiving = rx.recv_async();
        tokio::pin!(receiving);
        assert!(futures::poll!(&mut receiving).is_pending());
        reserved.send(Cmd::Shutdown);
        assert!(receiving.await.is_ok());
    }
}
