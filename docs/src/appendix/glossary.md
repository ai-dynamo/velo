# Glossary

| Term | Meaning |
|---|---|
| Active message | A message that names a handler on the remote instance. The handler runs when the message arrives. |
| Admission | The step where a transport puts an inbound request on the queue with `TransportAdapter::admit_message`. Admission takes the in-flight guard. |
| Admission gate | The per-target queue that holds sends while the bounded send channel is full |
| Anchor | The consumer end of a stream. It has a `u128` handle that a producer uses to attach. |
| Attach | The `_anchor_attach` round trip that connects a producer to an anchor |
| Batch | One messenger frame that carries records from many streams to one peer |
| Coalescing writer | The TCP, UDS, and QUIC writer task. It writes small queued frames together, and a large frame as two writes. |
| Credit | The number of records that a sender can send on a slot before the receiver grants more |
| Drain | The second phase of graceful shutdown. Velo waits until no admitted request is in flight. |
| Frame transport | A transport for stream frames when the mux is not used (TCP or gRPC) |
| Gate | The first phase of graceful shutdown. New inbound requests are refused. |
| `InstanceId` | The identity of one `Velo` instance |
| Mux | Batched streaming. Records from many streams share messenger frames to the same peer. |
| Ordered lane | For an ordered handler, the queue and task for one sender (or for all senders with `ordered_global`) |
| Pre-bind | `prebind_anchor`. The consumer makes a ticket, so the producer can open the stream with no attach round trip. |
| Primary transport | The compatible transport with the highest priority for a peer |
| Rendezvous | Transfer of a large payload by handle. The owner stages the bytes, and the peer pulls them. |
| RDMA GET | A one-sided read by the NIC of the peer from registered memory on the owner |
| Slot | The identity of one stream inside the mux, with its own order and credit |
| Teardown | The third phase of graceful shutdown. Velo cancels tokens and stops the transports. |
| Close | The fourth phase of graceful shutdown. Velo waits for `Transport::closed()` on each transport, so that what it wrote reaches the peer. |
| Ticket | A `StreamOpenTicket`. It carries the terms of a pre-bound stream. |
| `TransportKey` | The name of a transport in a `WorkerAddress`, for example `tcp` |
| `WorkerAddress` | A MessagePack map from `TransportKey` to endpoint bytes |
| `WorkerId` | The identity of a worker. A `StreamAnchorHandle` encodes it. |
