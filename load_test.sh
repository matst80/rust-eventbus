#!/bin/bash

# Configuration
NODE1_URL="http://127.0.0.1:3000/todos"
NODE2_URL="http://127.0.0.1:3001/todos"
CONCURRENCY=5
TOTAL_REQUESTS=200

echo "🚀 Starting Load Test on Distributed Todo App..."
echo "Total Requests: $TOTAL_REQUESTS (Concurrency: $CONCURRENCY)"

# Function to send a single request
send_request() {
    local i=$1
    local node_url=$2
    curl -s -X POST "$node_url" \
         -H "Content-Type: application/json" \
         -d "{\"title\": \"Automated Task $i from Node $node_url\"}" > /dev/null
}

# Export function for xargs/parallel
export -f send_request

# Run load test using xargs for simple concurrency
seq 1 "$TOTAL_REQUESTS" | xargs -I {} -P "$CONCURRENCY" bash -c "send_request {} $( (( RANDOM % 2 )) && echo $NODE1_URL || echo $NODE2_URL )"

echo "✅ Load test complete!"
echo "Final count on Node 1: $(curl -s http://127.0.0.1:3000/todos | jq '. | length' 2>/dev/null || echo 'unknown (install jq)')"
echo "Final count on Node 2: $(curl -s http://127.0.0.1:3001/todos | jq '. | length' 2>/dev/null || echo 'unknown (install jq)')"
