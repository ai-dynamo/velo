// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Byte items over the mux: `StreamSender<Bytes>::send_bytes` and
//! `StreamAnchor<Bytes>::next_bytes`, between two real nodes.
//!
//! These are for streams whose items the application already serialized, such
//! as response chunks. The typed path sends each one through serde twice: the
//! sender encodes it into a growing buffer, and the consumer's `Bytes`
//! `Deserialize` copies it into a new one. The byte path writes the frame by
//! hand and hands the consumer a slice of the buffer the frame arrived in.
//!
//! The byte path is not a second protocol. Its frames are the typed frames
//! byte for byte, so each end may use either path, and every frame that is
//! not an item goes through the same code on the consumer. These tests hold
//! both claims over a real wire, where a mismatch would show up as a payload
//! that differs or a stream that ends differently.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use velo::Velo;
use velo::streaming::{MESSENGER_MUX_KEY, StreamAnchor, StreamFrame, StreamSender};
use velo::transports::tcp::TcpTransportBuilder;

const PATIENCE: Duration = Duration::from_secs(30);

/// Payloads around each msgpack `bin` width, plus the empty and ~1 MiB
/// payloads a response stream sends (a token, a multimodal chunk).
fn payloads() -> Vec<Bytes> {
    [0usize, 1, 255, 256, 65_535, 65_536, 1 << 20, 100]
        .into_iter()
        .map(|len| {
            (0..len)
                .map(|i| (i % 251) as u8)
                .collect::<Vec<u8>>()
                .into()
        })
        .collect()
}

async fn node() -> Arc<Velo> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind loopback");
    let transport = TcpTransportBuilder::new()
        .from_listener(listener)
        .expect("from_listener")
        .build()
        .expect("build transport");
    // Mux only, the way a serving frontend and its workers run.
    Velo::builder()
        .add_transport(Arc::new(transport))
        .mux_only()
        .build()
        .await
        .expect("build velo")
}

/// Two cross-registered nodes: `consumer` owns the anchors, `producer` sends.
async fn pair() -> (Arc<Velo>, Arc<Velo>) {
    let consumer = node().await;
    let producer = node().await;
    consumer
        .register_peer(producer.peer_info())
        .expect("register producer");
    producer
        .register_peer(consumer.peer_info())
        .expect("register consumer");
    for (node, peer) in [(&producer, &consumer), (&consumer, &producer)] {
        tokio::time::timeout(
            PATIENCE,
            node.wait_for_handler(peer.instance_id(), "_anchor_attach"),
        )
        .await
        .expect("timed out waiting for the peer's control plane")
        .expect("peer never advertised the handler");
    }
    (consumer, producer)
}

/// How a producer gets its sender: the ways a serving frontend opens one.
#[derive(Debug, Clone, Copy)]
enum Open {
    /// Zero-RTT: the consumer pre-binds and the producer opens on the ticket.
    Ticket,
    /// The ordinary `_anchor_attach` round trip.
    Attach,
    /// A producer on the consumer's own node, which bypasses the mux.
    Local,
}

async fn open(
    consumer: &Velo,
    producer: &Velo,
    anchor: &StreamAnchor<Bytes>,
    how: Open,
) -> StreamSender<Bytes> {
    let handle = anchor.handle();
    let sender = match how {
        Open::Ticket => {
            let ticket = consumer.prebind_anchor(handle).expect("mux ticket");
            producer.open_anchor_stream::<Bytes>(handle, ticket).await
        }
        Open::Attach => producer.attach_anchor::<Bytes>(handle).await,
        Open::Local => consumer.attach_anchor::<Bytes>(handle).await,
    }
    .expect("open the sender");
    let expected = match how {
        Open::Local => None,
        Open::Ticket | Open::Attach => Some(MESSENGER_MUX_KEY),
    };
    assert_eq!(
        sender.negotiated_transport().map(|key| key.as_str()),
        expected,
        "{how:?}"
    );
    sender
}

/// Which consumer read to use.
#[derive(Debug, Clone, Copy)]
enum Read {
    Typed,
    Bytes,
}

async fn next(
    anchor: &mut StreamAnchor<Bytes>,
    read: Read,
) -> Option<Result<StreamFrame<Bytes>, velo::streaming::StreamError>> {
    let frame = match read {
        Read::Typed => anchor.next().await,
        Read::Bytes => anchor.next_bytes().await,
    };
    // A heartbeat must never surface on either path.
    assert!(!matches!(frame, Some(Ok(StreamFrame::Heartbeat))));
    frame
}

/// Every frame the consumer sees until the stream ends, as text, so the two
/// reads can be compared outcome for outcome.
async fn outcomes(anchor: &mut StreamAnchor<Bytes>, read: Read) -> Vec<String> {
    let collect = async {
        let mut seen = Vec::new();
        while let Some(frame) = next(anchor, read).await {
            seen.push(match frame {
                Ok(StreamFrame::Item(item)) => format!("Item({:?})", item.as_ref()),
                other => format!("{other:?}"),
            });
        }
        seen
    };
    tokio::time::timeout(PATIENCE, collect)
        .await
        .expect("the stream never ended")
}

/// Send `payloads` one way and read them the other, then `Finalized`, and the
/// end of the stream after it.
async fn round_trip(consumer: &Velo, producer: &Velo, how: Open, send_raw: bool, read: Read) {
    let mut anchor = consumer.create_anchor::<Bytes>();
    let sender = open(consumer, producer, &anchor, how).await;
    let sent = payloads();
    let send = {
        let sent = sent.clone();
        tokio::spawn(async move {
            for payload in sent {
                if send_raw {
                    sender.send_bytes(&payload).await.expect("send_bytes");
                } else {
                    sender.send(payload).await.expect("send");
                }
            }
            sender.finalize().expect("finalize");
        })
    };
    let collect = async {
        let mut items = Vec::new();
        loop {
            match next(&mut anchor, read).await {
                Some(Ok(StreamFrame::Item(item))) => items.push(item),
                Some(Ok(StreamFrame::Finalized)) => break,
                other => panic!("{how:?} {read:?}: unexpected {other:?}"),
            }
        }
        items
    };
    let items = tokio::time::timeout(PATIENCE, collect)
        .await
        .expect("timed out collecting items");
    send.await.expect("send task");
    assert_eq!(items, sent, "{how:?} raw={send_raw} {read:?}");
    assert!(
        next(&mut anchor, read).await.is_none(),
        "{how:?} {read:?}: nothing follows Finalized"
    );
}

/// Byte items cross the mux whole and in order, from an empty payload to
/// 1 MiB, on every way a sender opens.
///
/// 1 MiB plus its 11-byte frame header is over the default 1 MiB slot byte
/// budget, so the largest item also checks that one record above the cap is
/// still delivered rather than wedging the slot.
#[tokio::test(flavor = "multi_thread")]
async fn byte_items_round_trip() {
    let (consumer, producer) = pair().await;
    for how in [Open::Ticket, Open::Attach, Open::Local] {
        round_trip(&consumer, &producer, how, true, Read::Bytes).await;
    }
}

/// Each end may use either path, and the payloads come out the same.
///
/// `send_bytes` writes by hand the frame `send` writes through serde for a
/// `Bytes` item, and `next_bytes` decodes what `next` decodes. A frame that
/// differed by one header byte would fail here as a deserialization error or
/// a changed payload.
#[tokio::test(flavor = "multi_thread")]
async fn byte_and_typed_ends_interoperate() {
    let (consumer, producer) = pair().await;
    for how in [Open::Ticket, Open::Attach] {
        round_trip(&consumer, &producer, how, true, Read::Typed).await;
        round_trip(&consumer, &producer, how, false, Read::Bytes).await;
    }
}

/// What a producer does to a stream, besides sending items.
#[derive(Debug, Clone, Copy)]
enum Script {
    /// A soft error between items, then a clean finish.
    SoftErrorThenFinalize,
    /// The producer is dropped without finalizing.
    Drop,
    /// The producer detaches, and a second one attaches and finishes.
    DetachThenReattach,
}

async fn run(consumer: &Velo, producer: &Velo, script: Script, read: Read) -> Vec<String> {
    let mut anchor = consumer.create_anchor::<Bytes>();
    let sender = open(consumer, producer, &anchor, Open::Attach).await;
    let handle = anchor.handle();
    let producer_side = async move {
        match script {
            Script::SoftErrorThenFinalize => {
                sender.send_bytes(b"before").await.unwrap();
                sender.send_err("bad chunk").await.unwrap();
                sender.send_bytes(b"after").await.unwrap();
                sender.finalize().unwrap();
            }
            Script::Drop => {
                sender.send_bytes(b"last").await.unwrap();
                drop(sender);
            }
            Script::DetachThenReattach => {
                sender.send_bytes(b"first").await.unwrap();
                sender.detach().unwrap();
                // The attach flag clears once the consumer reads `Detached`,
                // and until then a remote attach is refused, so retry.
                let reattach = async {
                    loop {
                        match producer.attach_anchor::<Bytes>(handle).await {
                            Ok(sender) => break sender,
                            Err(_) => tokio::time::sleep(Duration::from_millis(5)).await,
                        }
                    }
                };
                let second = tokio::time::timeout(PATIENCE, reattach)
                    .await
                    .expect("the anchor never took a second sender");
                second.send_bytes(b"second").await.unwrap();
                second.finalize().unwrap();
            }
        }
    };
    let (seen, ()) = tokio::join!(outcomes(&mut anchor, read), producer_side);
    seen
}

/// Every frame that is not an item does the same thing on both reads.
///
/// `next_bytes` shares its frame handling with `next`; only the item decode
/// differs. A soft error must not end the stream, `Dropped` and `Finalized`
/// must, `Detached` must leave the anchor open for the next sender, and
/// nothing may follow the end. Each script runs once per read and the two
/// outcome lists must match exactly.
#[tokio::test(flavor = "multi_thread")]
async fn non_item_frames_end_the_stream_the_same_way() {
    let (consumer, producer) = pair().await;
    let item = |payload: &[u8]| format!("Item({payload:?})");
    for (script, expected) in [
        (
            Script::SoftErrorThenFinalize,
            vec![
                item(b"before"),
                "Err(SenderError(\"bad chunk\"))".to_string(),
                item(b"after"),
                "Ok(Finalized)".to_string(),
            ],
        ),
        (
            Script::Drop,
            vec![item(b"last"), "Err(SenderDropped)".to_string()],
        ),
        (
            Script::DetachThenReattach,
            vec![
                item(b"first"),
                "Ok(Detached)".to_string(),
                item(b"second"),
                "Ok(Finalized)".to_string(),
            ],
        ),
    ] {
        let typed = run(&consumer, &producer, script, Read::Typed).await;
        let bytes = run(&consumer, &producer, script, Read::Bytes).await;
        assert_eq!(typed, expected, "{script:?}");
        assert_eq!(bytes, typed, "{script:?}");
    }
}
