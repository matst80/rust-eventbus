# rust-eventbus — Event Bus & TCP Mesh (Pub/Sub)

This document explains the in-repo Event Bus and the zero-dependency TCP-based Pub/Sub (mesh) used for cross-node event distribution. It summarizes the core types, runtime flow, configuration, and how the example `todo_app` wires everything together.

**Core Concepts**
- **Event**: `Event<T>` is a typed wrapper carrying metadata and the domain payload (`T: EventPayload`). It contains `id`, `aggregate_id`, `sequence_num`, `global_sequence_num`, `timestamp`, and `payload`.
- **EventPayload**: A trait that domain event enums implement (Serialize/Deserialize + `event_type()`), enabling safe, strongly-typed event handling and serde-based (de)serialization.

**Local Event Bus (`EventBus`)**
- **Implementation**: Uses `tokio::sync::broadcast`.
- **API**:
  - `EventBus::new(capacity)` — creates a bus with a bounded capacity.
  - `publish(event)` — sends an `Event<T>` to all active subscribers; returns the number of receivers that received it.
  - `subscribe()` — returns a `broadcast::Receiver<Event<T>>` which receives future published events.
- **Memory safety & backpressure**:
  - `broadcast::Receiver` is automatically unsubscribed when dropped — no manual unsubscribe needed.
  - If a receiver lags behind by more than `capacity`, `RecvError::Lagged` occurs (explicit backpressure signal).

**TCP Mesh (DistributedPubSub / `TcpPubSub`)**
- **Purpose**: A lightweight, peer-to-peer TCP-based mesh for broadcasting events between nodes without external dependencies.
- **Discovery**:
  - `NodeDiscovery` trait abstracts discovery.
  - `EnvironmentNodeDiscovery` reads `PEERS` env var (comma-separated addresses).
  - `DnsNodeDiscovery` performs DNS lookups (useful with Kubernetes headless services).
- **Publish flow** (`TcpPubSub::publish`):
  - Serializes `Event<T>` with `bincode`.
  - Prepends a 4-byte length and the sender `Uuid` (16 bytes) so receivers can filter self-sent events.
  - Uses a connection cache (`connections: HashMap<String, TcpStream>`) to reuse open TCP sockets.
  - For each discovered peer (excluding the node's own advertised endpoint) it either reuses the stream or attempts to connect and send the payload in a background task.
  - Non-blocking: sends are spawned onto tasks per-peer with short timeouts and best-effort semantics.
- **Subscribe flow** (`TcpPubSub::subscribe`):
  - Binds a `TcpListener` on the mesh bind address and accepts connections.
  - Each accepted connection reads a 4-byte length then reads the body; it expects at least 16 bytes (sender UUID) followed by the `bincode` event payload.
  - If the sender UUID equals the local node ID the message is ignored (dedupe self-origin).
  - Deserializes the payload and forwards an `Event<T>` into an mpsc channel exposed as a `BoxStream`.

**Why this design**
- Local `EventBus` is great for low-latency intra-process fan-out (projections, in-memory subscriptions).
- `TcpPubSub` provides a minimal, dependency-free cross-node event distribution that integrates seamlessly:
  - Nodes publish events to their local store, then publish to both the local bus and the mesh.
  - Incoming mesh events are pushed into the local `EventBus`, so existing projection actors and consumers process events uniformly whether they originated locally or remotely.

**Projections: Ephemeral vs Durable**
- **EphemeralProjectionActor**: subscribes to the local `EventBus` and maintains an in-memory read model on every node (API-facing state). It replays stored events at start and then applies live events.
- **DurableProjectionActor**: designed to run only on a single node at a time for side-effects (e.g., sending emails). It uses a `ProjectionLockManager` (example: `FileLeaseLockManager`) to implement a simple lease/lock so only one node holds the projection responsibility.

**Example flow (from `examples/todo_app.rs`)**
1. Client POST /todos -> server creates `Event::new(...)` for `TodoCreated`.
2. Server appends the event to the `FileEventStore` (durable storage).
3. After successful append, the server:
   - publishes the stored event to the local `EventBus` (fast intra-node distribution), and
   - calls `mesh.publish(&event).await` to send it to peers.
4. Each peer that receives the mesh message deserializes it and sends it into its local `EventBus` (as if the event was published locally).
5. Projection actors subscribed to the local `EventBus` update read models or run side effects.

**Config / Env vars used by the example app**
- `PORT` — HTTP API port (default 3000)
- `MESH_PORT` — mesh listen port (default 3001)
- `HOST` — bind host for HTTP
- `MESH_BIND_HOST` — mesh bind host (defaults to `HOST`)
- `MESH_ADVERTISE_HOST` / `POD_IP` — address announced to peers (used with DNS discovery / k8s)
- `DATA_DIR` — directory for event log and snapshots
- `NODE_ID` — optional, supply a UUID to identify node (auto-generated otherwise)
- `PEERS` — comma-separated peer addresses for `EnvironmentNodeDiscovery` (e.g., `127.0.0.1:3001,127.0.0.1:3002`)
- `DNS_QUERY` — when set, `DnsNodeDiscovery` will lookup targets for clustering

**Operational notes & trade-offs**
- The mesh is *best-effort*: publishes spawn background tasks and will not block the HTTP path for every peer. It logs success/failure and attempts reconnection when needed.
- Events are serialized with `bincode` for compact transit; a 4-byte length prefix + sender UUID is used to frame messages.
- There is no built-in reliable delivery or guaranteed ordering across nodes — if you need stronger guarantees, add acknowledgements, persistent queues, or an external broker.
- The `EventBus` capacity controls per-subscriber buffering; lagging subscribers will observe `Lagged` errors and must handle them.

**Reading the code (helpful file pointers)**
- `src/event.rs` — `Event<T>` and `EventPayload` trait (payload contract).
- `src/bus.rs` — `EventBus<T>` (local pub/sub with `tokio::sync::broadcast`).
- `src/distributed.rs` — `DistributedPubSub` trait, `EnvironmentNodeDiscovery`, `DnsNodeDiscovery`, `TcpPubSub` (mesh pub/sub), and helper utilities.
- `examples/todo_app.rs` — end-to-end example wiring storage, bus, mesh, and projections.

**Quick try (local)**
1. Start two nodes with distinct ports and mesh ports, e.g.: 

```bash
# Terminal 1
PORT=3000 MESH_PORT=3001 PEERS=127.0.0.1:3002 cargo run --example todo_app

# Terminal 2
PORT=3002 MESH_PORT=3003 PEERS=127.0.0.1:3001 NODE_ID=$(uuidgen) cargo run --example todo_app
```

2. Create a todo on node1:

```bash
curl -X POST -H "Content-Type: application/json" -d '{"title":"Test from node1"}' http://127.0.0.1:3000/todos
```

You should see the event appended locally on node1, sent to node2 over the mesh, and applied to both nodes' projections.

**Extending / Hardening ideas**
- Implement optional per-message ACKs and retries for reliable delivery.
- Add sequence/gap detection for remote events and replays from a durable source to repair missed events.
- Use TLS or mTLS for mesh connections in untrusted networks.
- Replace `bincode` with a versioned wire format (or add explicit event version metadata) for long-lived clusters.

---
Saved to: README_EVENTBUS.md
