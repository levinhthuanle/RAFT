#!/bin/bash

NUM_NODES=${1:-3}
BASE_PORT=8000

cleanup() {
    echo "Stopping all nodes..."
    kill "${PIDS[@]}" 2>/dev/null
}
trap cleanup EXIT

cd "$(dirname "$0")/../server"

PIDS=()
for i in $(seq 1 $NUM_NODES); do
    PORT=$((BASE_PORT + i))
    echo "Starting node $i on port $PORT"
    python main.py --node-id "$i" --port "$PORT" &
    PIDS+=($!)
done

echo ""
echo "$NUM_NODES nodes running. Press Ctrl+C to stop all."
wait