# Distributed Key-Value Store

A fault-tolerant, multi-threaded distributed key-value store written in Java with gRPC.
Nodes elect a leader, replicate every write to a majority before acknowledging it,
survive node crashes, and recover their data from a write-ahead log on restart.
A cluster-aware client spreads read traffic across replicas to cut latency under load.

![CI](https://github.com/champion-buzz/distributed-kv-store/actions/workflows/ci.yml/badge.svg)

## Features

- **gRPC API**: `Get`, `Put`, `Delete` and `ClusterInfo` for clients, plus an internal `Replication` service (`Heartbeat`, `Replicate`, `RequestVote`, `Snapshot`).
- **Leader coordination (simplified Raft)**: heartbeats, randomized election timeouts, term numbers, and "only vote for an up-to-date candidate".
- **Quorum replication**: the leader fans each write out to followers in parallel and replies only after a majority has acked. One ordered queue per follower keeps entries in sequence.
- **Fault tolerance**: if the leader dies, a new one is elected and clients re-discover it automatically. A follower that falls behind pulls a snapshot from the leader.
- **Durability**: every applied entry is appended to a per-node write-ahead log (WAL) and replayed on startup.
- **Multi-threaded**: lock-free reads (`ConcurrentHashMap`), a bounded gRPC worker pool per node, non-blocking write forwarding, and parallel `multiGet`.
- **Client-side load balancing**: `SINGLE_NODE` (baseline), `ROUND_ROBIN`, and `LEAST_OUTSTANDING` (power-of-two-choices on in-flight requests).

## Architecture

```
              ┌──────────── KVClient (load-balanced reads, leader-aware writes) ───────────┐
              │                              │                                             │
        Get/Put/Delete                 Get/Put/Delete                                Get/Put/Delete
              ▼                              ▼                                             ▼
     ┌─────────────────┐  Replicate   ┌─────────────────┐   Replicate          ┌─────────────────┐
     │  Node 2 (LEADER)│─────────────▶│ Node 1 (FOLLOWER)│◀────────────────────│ Node 3 (FOLLOWER)│
     │  KVStore + WAL  │  Heartbeat   │  KVStore + WAL   │  (writes sent to a  │  KVStore + WAL   │
     └─────────────────┘─────────────▶└─────────────────┘   follower are      └─────────────────┘
                                                             forwarded to the leader)
```

| File | Role |
|---|---|
| `src/main/proto/kvstore.proto` | Service and message definitions |
| `server/Node.java` | Election, heartbeats, replication, gRPC services |
| `server/KVStore.java` | Thread-safe state machine + write-ahead log |
| `server/LocalCluster.java` | Runs an N-node cluster in one JVM (tests, demo, benchmark) |
| `client/KVClient.java` | Load-balancing, failover-aware client |
| `client/Cli.java` | Interactive shell |
| `bench/Benchmark.java` | Concurrent load test comparing routing strategies |

## Getting started

Requirements: Java 17+ and Maven 3.8+.

```bash
mvn package                 # builds target/kvstore.jar and runs the test suite
```

**Option A: whole cluster in one process**

```bash
java -cp target/kvstore.jar com.kvstore.server.LocalCluster 3
```

**Option B: three separate processes**

```bash
./scripts/start-cluster.sh
# or by hand:
java -cp target/kvstore.jar com.kvstore.server.Node --id 1 --port 50051 --peers 2=localhost:50052,3=localhost:50053
```

**Talk to it**

```bash
java -cp target/kvstore.jar com.kvstore.client.Cli localhost:50051,localhost:50052,localhost:50053
kv> put city Tempe
OK
kv> get city
Tempe
kv> info
node 1  leader=2  term=1  index=1  keys=1
node 2  leader=2  term=1  index=1  keys=1
node 3  leader=2  term=1  index=1  keys=1
```

Try killing the leader process: within about a second the survivors elect a new leader and writes keep working.

## Tests

`ClusterTest` checks the following:
- exactly one leader is elected, and writes and deletes reach every replica
- 400 concurrent writes from 16 threads end up consistent on all nodes
- the cluster keeps serving reads and writes after the leader is killed
- state is rebuilt from the WAL after a full cluster restart

## Benchmark

```bash
java -Dkvstore.readCostMicros=500 -cp target/kvstore.jar com.kvstore.bench.Benchmark 64 300 4
#                                                                         threads ops/thread workers/node
```

The benchmark starts a 3-node cluster, loads 1,000 keys, then runs 64 client threads doing 90% reads and 10% writes.
Every routing strategy gets the same workload, and the output shows p50/p99/mean latency and throughput for each one.
`readCostMicros` simulates the storage cost of a disk-backed node; each node has a fixed number of worker threads.
Under this load, sending every request to one node makes requests queue up. Spreading reads across replicas uses all three nodes' capacity.

CI runs the benchmark on every push. You can find the latest numbers in the **Actions → CI → Summary** tab.

## Design notes and simplifications

This is a learning project, not a production database. It intentionally simplifies Raft:
- Followers apply entries as soon as they receive them, instead of waiting for a separate commit index.
- Lagging followers catch up by full snapshot rather than log back-filling.
- Follower reads are eventually consistent. Read from the leader if you need read-your-writes.
- Cluster membership is static (set at startup).
