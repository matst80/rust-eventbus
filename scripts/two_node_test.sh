#!/usr/bin/env bash
set -euo pipefail

# Simple two-node test runner for the todo_app example.
# Starts two nodes on different ports and mesh ports, creates a todo on node A,
# then queries node B to verify replication.

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT_DIR"

NODE_A_LOG=tmp/nodeA.log
NODE_B_LOG=tmp/nodeB.log
mkdir -p tmp

echo "Starting node A (API:3000, MESH:3001)..."
NODE_A_PORT=3000 NODE_A_MESH=3001 \
PEERS=127.0.0.1:3003 NODE_ID=$(uuidgen) \
nohup cargo run --example todo_app > "$NODE_A_LOG" 2>&1 &
NODE_A_PID=$!

echo "Starting node B (API:3002, MESH:3003)..."
NODE_B_PORT=3002 NODE_B_MESH=3003 \
PORT=$NODE_B_PORT MESH_PORT=$NODE_B_MESH \
PEERS=127.0.0.1:3001 NODE_ID=$(uuidgen) \
nohup cargo run --example todo_app > "$NODE_B_LOG" 2>&1 &
NODE_B_PID=$!

# Wait for nodes to boot
sleep 3

echo "Creating todo on node A"
curl -s -X POST -H "Content-Type: application/json" -d '{"title":"hello from A"}' http://127.0.0.1:3000/todos || true

# Allow mesh to deliver
sleep 2

echo "Fetching todos from node B"
curl -s http://127.0.0.1:3002/todos || true

echo "Logs: $NODE_A_LOG, $NODE_B_LOG"

echo "PIDs: $NODE_A_PID, $NODE_B_PID"

echo "To stop nodes: kill $NODE_A_PID $NODE_B_PID"
