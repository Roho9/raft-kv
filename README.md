# raft-kv

A fault-tolerant distributed key-value store built from scratch in Go on the
[Raft consensus algorithm](https://raft.github.io/raft.pdf). A 3-node cluster
survives any single-node failure while serving fully linearizable reads and
writes over gRPC.

## Features

- **Raft consensus from scratch**: leader election with randomized timeouts,
  log replication with the conflict-backoff optimization, and a leader no-op
  entry per term for prompt commitment (no external consensus libraries).
- **Durability**: a CRC-framed write-ahead log fsynced on every append batch,
  atomic metadata writes, and crash recovery that truncates torn tail writes.
- **Snapshotting**: automatic log compaction once the log passes a threshold,
  with InstallSnapshot shipping state to followers that fall too far behind.
- **Linearizability**: every operation, including reads, is replicated
  through the Raft log and executed at its commit point.
- **Idempotent client semantics**: each client session tags operations with
  a unique id and sequence number; the state machine applies each pair at
  most once, so retries across leader failovers never double-apply. Session
  state is included in snapshots so dedup survives compaction.
- **Partition testing**: the transport can drop traffic between arbitrary
  node pairs in both directions, letting the test suite verify consistency
  through simulated network partitions.
- **Containerized**: one static binary per node, with a Docker Compose file
  that brings up a reproducible 3-node cluster.

## Layout

```
proto/       protobuf definitions for Raft RPCs and the KV service
gen/         generated protobuf and gRPC code
raft/        consensus core: elections, replication, storage, snapshots
kv/          replicated state machine with client session dedup
server/      node wiring: gRPC services, apply loop, snapshot trigger
client/      cluster-aware client with leader discovery and retries
cmd/kvnode   node daemon
cmd/kvbench  load generator reporting throughput and latency percentiles
tests/       end-to-end cluster tests (failover, partitions, snapshots)
```

## Quick start

Requires Go 1.22 or newer.

```sh
make build          # builds bin/kvnode and bin/kvbench
make cluster        # starts a local 3-node cluster on ports 7001-7003
```

In another terminal:

```sh
make bench          # 64 clients, 50k mixed ops against the local cluster
```

Or with Docker:

```sh
make docker-up      # builds the image and starts 3 containers
docker compose run --rm --entrypoint kvbench node1 \
    --addrs node1:7001,node2:7002,node3:7003 --ops 20000
make docker-down
```

## Using the client

```go
cl, _ := client.New([]string{"localhost:7001", "localhost:7002", "localhost:7003"})
defer cl.Close()

ctx := context.Background()
cl.Put(ctx, "greeting", []byte("hello"))
value, found, _ := cl.Get(ctx, "greeting")   // linearizable read
cl.Delete(ctx, "greeting")
```

The client discovers the leader by following redirect hints, rotates through
nodes on failure, and retries each logical operation with a fixed sequence
number so a retry can never apply twice.

## Tests

```sh
make test           # full suite with the race detector
```

The suite covers:

- leader election and re-election after leader death
- durability of committed writes across leader failover
- an isolated leader stepping down and the majority side continuing to commit
- snapshot-based catch-up of a follower that missed compacted log entries
- full-cluster restart recovery from the WAL and snapshots
- 10,000 concurrent writes with a mid-run leader partition, verifying every
  acknowledged write is durable and reads return the last acknowledged value
- at-most-once application of retried requests

## Benchmarking

`kvbench` drives concurrent client sessions and reports throughput with
latency percentiles:

```
bin/kvbench --addrs localhost:7001,localhost:7002,localhost:7003 \
    --clients 64 --ops 50000 --read-ratio 0.5 --value-size 128
```

Every operation (reads included) is committed through Raft, so results
measure full consensus round trips: WAL sync on the leader plus replication
to a follower quorum. Numbers depend on hardware and fsync behavior; run it
on your own machine for honest figures.

Measured on an Apple M3 (macOS, all 3 nodes plus clients on one machine,
100k ops, 50 percent reads, 128 byte values):

| clients | throughput | p50 | p95 | p99 |
|--------:|-----------:|-------:|-------:|--------:|
| 16 | 22.9K ops/sec | 0.56ms | 1.13ms | 2.77ms |
| 64 | 32.6K ops/sec | 1.49ms | 3.72ms | 12.25ms |
| 256 | 38.4K ops/sec | 4.90ms | 14.79ms | 39.65ms |

Throughput comes from group commit: the leader appends proposals to the WAL
immediately but lets a dedicated syncer goroutine batch many proposals into
one fsync, and an entry only counts toward the commit quorum on a node once
it is durable there. By default WAL syncs use plain fsync (fdatasync on
Linux), which survives process crashes; pass `--full-fsync` to force full
disk cache flushes (F_FULLFSYNC on macOS, roughly 10ms per flush) if you
want single-node power-loss durability at a large throughput cost.

## Design notes

- **Why replicate reads?** Serving reads from the leader's local state
  without a log entry can violate linearizability during a partition (a
  deposed leader might serve stale data). Replicating reads is the simplest
  correct approach; ReadIndex or leader leases would be the standard
  optimizations if read throughput mattered more.
- **Storage** is a purpose-built append-only log rather than an embedded
  database: entries are length-prefixed and CRC-checked, metadata files are
  written via temp-file rename, and recovery drops any torn record at the
  tail, which can only ever be an unacknowledged write.
- **Snapshots** include the client session table, so exactly-once semantics
  hold even for requests that straddle a compaction.

## License

MIT
