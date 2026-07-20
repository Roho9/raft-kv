#!/usr/bin/env bash
# Starts a local 3-node cluster on ports 7001-7003. Ctrl-C stops all nodes.
set -euo pipefail
cd "$(dirname "$0")/.."

PEERS="1=localhost:7001,2=localhost:7002,3=localhost:7003"
mkdir -p data

pids=()
cleanup() {
    for pid in "${pids[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

for id in 1 2 3; do
    ./bin/kvnode --id "$id" --peers "$PEERS" --data-dir "data/n$id" &
    pids+=($!)
done

echo "cluster running on ports 7001-7003 (ctrl-c to stop)"
wait
