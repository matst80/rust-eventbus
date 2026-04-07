# Rust EventBus & RAG Pipeline

A robust, asynchronous event bus and projection manager with built-in RAG (Retrieval-Augmented Generation) capabilities, including web crawling, ONNX-based embeddings, and graph-based search.

## 🚀 Quick Start: Running the Examples

This project includes several examples demonstrating the core event bus, the mesh network, and the RAG pipeline.

### 1. RAG Pipeline: Crawling & Embedding
The main pipeline that crawls a website, chunks the content, generates embeddings (using ONNX), and builds a knowledge graph.
```bash
# This will crawl the Rust Book by default and save state to examples/outputs/
cargo run --example graph_pipeline
```

### 2. Interactive Search (TUI)
A Terminal User Interface to explore the generated knowledge graph and perform similarity searches.
```bash
# Requires graph_pipeline to have been run first
cargo run --example search_tui
```

### 3. Reranking & Two-Stage Retrieval
A demonstration of how to improve retrieval quality by using a second-stage reranker after the initial vector search.
```bash
# Explains Bi-Encoders vs Cross-Encoders with a live comparison
cargo run --example reranking_example
```

### 4. Distributed Todo App
Demonstrates the event bus mesh network with two nodes syncing events over TCP.
```bash
# Terminal 1
PORT=3000 MESH_PORT=3001 PEERS=127.0.0.1:3002 cargo run --example todo_app

# Terminal 2 (in a separate window)
PORT=3002 MESH_PORT=3003 PEERS=127.0.0.1:3001 NODE_ID=$(uuidgen) cargo run --example todo_app
```

### 5. Other Utilities
- **Crawl Demo**: Simple CLI crawler outputting to console.
  ```bash
  cargo run --example crawl_demo
  ```
- **Search Demo**: Minimal CLI search tool.
  ```bash
  cargo run --example search_demo
  ```
- **Verify Chunking**: Test the logic used to split markdown into chunks.
  ```bash
  cargo run --example verify_chunking
  ```

---

## 🛠 Project Structure

- `src/bus.rs`: Local event bus using `tokio::sync::broadcast`.
- `src/distributed.rs`: TCP mesh for cross-node event distribution.
- `src/embedding/`: ONNX inference service for text embeddings (BGE-small).
- `src/crawler/`: Headless browser crawler using `chromiumoxide`.
- `src/graph/`: State management for the knowledge graph.

For more detailed documentation on the event bus internals, see [README_EVENTBUS.md](./README_EVENTBUS.md).
