// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Bounded command queue with an explicit receiver-close boundary.
//!
//! Closing refuses new submissions. Draining also waits for a sender which
//! reserved capacity before close, so accepted commands cannot miss teardown.
//! Tokio provides this boundary; Flume cannot close a receiver and retain its queue.

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
    pub fn try_send(&self, cmd: Cmd) -> Result<(), mpsc::error::TrySendError<Cmd>> {
        self.0.try_send(cmd)
    }

    pub async fn send_async(&self, cmd: Cmd) -> Result<(), mpsc::error::SendError<Cmd>> {
        self.0.send(cmd).await
    }
}

impl CommandReceiver {
    pub fn try_recv(&mut self) -> Result<Cmd, mpsc::error::TryRecvError> {
        self.0.try_recv()
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
    pub async fn recv_async(&mut self) -> Option<Cmd> {
        self.0.recv().await
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
        assert!(rx.recv_async().await.is_some());
        assert!(rx.recv_async().await.is_none());
        assert!(tx.try_send(Cmd::Shutdown).is_err());

        let (tx, mut rx) = command_ring(1);
        let reserved = tx.0.reserve().await.unwrap();
        rx.close();
        let receiving = rx.recv_async();
        tokio::pin!(receiving);
        assert!(futures::poll!(&mut receiving).is_pending());
        reserved.send(Cmd::Shutdown);
        assert!(receiving.await.is_some());
    }
}
