#!/bin/bash

# Target process name
PROCESS_NAME="todo_app"

# Find the PID
PID=$(pgrep "$PROCESS_NAME" | head -n 1)

if [ -z "$PID" ]; then
    echo "❌ Error: Process '$PROCESS_NAME' not found!"
    echo "Make sure the app is running (e.g., cargo run --example todo_app)"
    exit 1
fi

echo "🔍 Monitoring memory leaks for PID $PID ($PROCESS_NAME)..."
echo "Press [CTRL+C] to stop."

while true; do
    echo "--------------------------------------------------"
    date
    # Run leaks command (macOS specific)
    # Filter for the summary part to keep it clean
    leaks "$PID" | grep -E "Process $PID:|leaked bytes|malloced"
    
    echo "Memory footprint: $(ps -o rss= -p "$PID" | awk '{print $1/1024 " MB"}')"
    
    sleep 5
done
