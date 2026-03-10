# Implementation Plan — pluggable Pub/Sub adapters

This file outlines two parallel implementation paths: a libp2p (Gossipsub) path for a production-grade, decentralized mesh, and an async-nats path for brokered, durable messaging. Each path has milestones so we can implement them one at a time.

---

## Goals
- Provide a common adapter trait so the rest of the codebase (projections, store, examples) can plug different Pub/Sub implementations without further changes.
- Ship a working `examples/` demo for each backend.
- Keep the existing `TcpPubSub` implementation as a simple reference/learning implementation.

---

## Adapter design (shared)
- Trait: `DistributedPubSub` (exists in `src/distributed.rs`) — refine to expose async `publish(&self, event: EventBytes) -> Result<()>` and `subscribe(&self) -> BoxStream<EventBytes>`.
- Provide a small `EventCodec` helper that maps `Event<T>` <-> bytes (bincode) and wraps framing details (length/uuid) for non-libp2p transports.
- Provide `impl From`/`TryFrom` adapters to convert to/from `Event<T>` for the local bus.

---


---


## Path B — async-nats (brokered, durable)
Milestones:
1. Add dependency: `async-nats` (and optional `nats` features if needed).
2. Prototype `examples/nats_demo.rs` that connects to a local NATS server (or Docker), publishes, and subscribes to a subject.
3. Implement `NatsAdapter` implementing `DistributedPubSub`:
   - `publish(bytes)` publishes to a subject (topic per aggregate or single global subject configurable).
   - `subscribe()` creates a durable or ephemeral subscription and exposes messages as a `BoxStream`.
   - Optional: support JetStream for durability/ack semantics (separate milestone).
4. Wire `examples/todo_app.rs` to use `NatsAdapter` behind the trait for a demo.
5. Docs: running NATS locally (docker-compose snippet), config options for subjects vs streams.

Estimated effort: small–medium (depends on JetStream integration).

---

## Non-functional tasks
- Add feature flags in `Cargo.toml` (`libp2p-backend`, `nats-backend`) so users can enable only needed dependencies.
- Add CI example runs where feasible (unit tests + example smoke tests).
- Keep `TcpPubSub` as `--features tcp-backend` for learning/legacy.

---

## Next immediate steps (this iteration)
- Finalize the adapter trait signatures in `src/distributed.rs`.

---

Saved by: developer task — start implementing one path at a time.
