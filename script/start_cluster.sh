#!/bin/bash

NUM_NODES=${1:-3}
BASE_PORT=8000

# Kill any nodes we started when this script exits
cleanup() {
    echo "Stopping all nodes..."
    kill "${PIDS[@]}" 2>/dev/null
}
trap cleanup EXIT

cd "$(dirname "$0")/.."

PIDS=()
for i in $(seq 0 $((NUM_NODES - 1))); do
    PORT=$((BASE_PORT + i))
    echo "Starting node $i on port $PORT"
    python -m server.main --node-id "$i" --port "$PORT" &
    PIDS+=($!)
done

echo ""
echo "$NUM_NODES nodes running. Press Ctrl+C to stop all."
wait