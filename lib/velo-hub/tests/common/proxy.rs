// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! A TCP proxy a test can cut and restore, or point at another address.
//!
//! Between a hub and etcd it stands for the network: `Refuse` is a host
//! that is down (connections are refused, open ones are reset), `Blackhole`
//! is a partition (connections stay open and nothing arrives). In front of
//! hubs it stands for one address whose target moves, as a VIP does.

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

/// What the proxy does with connections.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// Copy bytes both ways.
    Forward,
    /// No listener: new connections are refused, open ones are closed.
    Refuse,
    /// Connections stay open, and nothing is copied either way.
    Blackhole,
}

#[derive(Clone, Debug)]
struct State {
    mode: Mode,
    upstream: String,
    /// Bumped when the upstream changes, so open connections end.
    generation: u64,
}

/// A running proxy. It stops when the test's runtime stops.
pub struct Proxy {
    port: u16,
    state: watch::Sender<State>,
}

impl Proxy {
    /// Listen on a free port and forward to `upstream` (`host:port`).
    pub fn start(upstream: impl Into<String>) -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let port = listener.local_addr().unwrap().port();
        let (state, receiver) = watch::channel(State {
            mode: Mode::Forward,
            upstream: upstream.into(),
            generation: 0,
        });
        tokio::spawn(serve(port, listener, receiver));
        Self { port, state }
    }

    /// Change what the proxy does from now on.
    pub fn set(&self, mode: Mode) {
        self.state.send_modify(|state| state.mode = mode);
    }

    /// Forward to `upstream` from now on, and close every open connection,
    /// so a pooled one cannot keep reaching the old target.
    pub fn point_to(&self, upstream: impl Into<String>) {
        let upstream = upstream.into();
        self.state.send_modify(|state| {
            state.upstream = upstream;
            state.generation += 1;
        });
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    /// `http://127.0.0.1:{port}`.
    pub fn url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }
}

async fn serve(port: u16, first: std::net::TcpListener, mut state: watch::Receiver<State>) {
    let mut listener = Some(TcpListener::from_std(first).unwrap());
    loop {
        while state.borrow().mode == Mode::Refuse {
            listener = None;
            if state.changed().await.is_err() {
                return;
            }
        }
        let current = match listener.take() {
            Some(listener) => listener,
            None => TcpListener::bind(("127.0.0.1", port)).await.unwrap(),
        };
        loop {
            tokio::select! {
                accepted = current.accept() => {
                    let Ok((client, _)) = accepted else { continue };
                    tokio::spawn(pipe(client, state.clone()));
                }
                changed = state.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    if state.borrow().mode == Mode::Refuse {
                        break;
                    }
                }
            }
        }
    }
}

async fn pipe(mut client: TcpStream, mut state: watch::Receiver<State>) {
    let (upstream, generation) = {
        let now = state.borrow_and_update();
        (now.upstream.clone(), now.generation)
    };
    if state.borrow().mode == Mode::Blackhole {
        // Accepted, then silent.
        let _ = state.wait_for(|now| now.mode != Mode::Blackhole).await;
        return;
    }
    let Ok(mut server) = TcpStream::connect(&upstream).await else {
        return;
    };
    let cut = tokio::select! {
        _ = tokio::io::copy_bidirectional(&mut client, &mut server) => None,
        changed = state.wait_for(|now| now.mode != Mode::Forward || now.generation != generation) => {
            changed.ok().map(|now| now.mode)
        }
    };
    if cut == Some(Mode::Blackhole) {
        let _ = state.wait_for(|now| now.mode != Mode::Blackhole).await;
    }
}
