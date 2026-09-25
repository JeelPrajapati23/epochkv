# KV Store

A distributed key-value store (Redis-like) built from scratch in C++, with a Python client and benchmark harness.

## Features

- [x] Project setup (CMake, Catch2 test harness, minimal blocking TCP server)
- [x] Core single-node store (RESP protocol parser, hash table, command dispatcher)
- [x] Concurrency (epoll-based event loop)
- [x] TTL & LRU eviction
- [x] Persistence (AOF + snapshotting)
- [x] Replication (master-replica)
- [x] Sharding (Redis Cluster-style hash slots, gossip, live resharding)
- [x] Fault tolerance (failure detection and automatic failover by replica election)
- [ ] Python client + benchmark harness (+ optional vector-search extension)
- [ ] Benchmarks and documentation

Supported commands: `PING`, `ECHO`, `SET` (with `EX`/`PX`/`EXAT`/`PXAT`/`NX`/`XX`/`KEEPTTL`), `GET`, `DEL`, `EXISTS`, `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `TTL`, `PTTL`, `PERSIST`, `DBSIZE`, `SAVE`, `BGSAVE`, `BGREWRITEAOF`, `LASTSAVE`, `REPLICAOF`, `WAIT`, `INFO`, `DUMP`, `RESTORE`, `MIGRATE`, `CLUSTER` (`INFO`, `NODES`, `SLOTS`, `MYID`, `MEET`, `ADDSLOTS[RANGE]`, `DELSLOTS[RANGE]`, `SETSLOT`, `KEYSLOT`, `COUNTKEYSINSLOT`, `GETKEYSINSLOT`, `REPLICATE`, `FORGET`, `FAILOVER`, `COUNT-FAILURE-REPORTS`, `SET-CONFIG-EPOCH`, `BUMPEPOCH`, `SAVECONFIG`), `ASKING`, `READONLY`, `READWRITE`.

## Build and run

Requires Linux (epoll), CMake 3.20+, and a C++17 compiler.

```sh
cmake -S . -B build
cmake --build build -j
./build/src/kv_server                  # listens on 127.0.0.1:6380
./build/src/kv_server --bind 0.0.0.0 --port 7000
./build/src/kv_server --maxmemory 100mb --maxmemory-policy allkeys-lru
./build/src/kv_server --dir /var/lib/kv --appendonly yes --appendfsync everysec
```

Any Redis client works, e.g. `redis-cli -p 6380`.

## Persistence

Two mechanisms, as in Redis, both off the hot path:

- **Snapshots** (`dump.snap` in `--dir`): a checksummed binary dump of the keyspace. `BGSAVE`, and the save points (`--save "3600 1 300 100 60 10000"` by default, `--save ""` to disable), `fork()` a child that writes the child's copy-on-write view of memory while the parent keeps serving. A final snapshot is written on `SIGTERM`/`SIGINT`.
- **Append-only file** (`--appendonly yes`): every write is logged as a RESP command, with relative TTLs rewritten to absolute deadlines. It's flushed before replies go out and fsynced per `--appendfsync always|everysec|no`. Uses the multi-part layout (`appendonlydir/`: manifest + base snapshot + incremental logs); `BGREWRITEAOF` and automatic rewrites (`--auto-aof-rewrite-percentage`, `--auto-aof-rewrite-min-size`) compact it in a forked child.

On startup the AOF is loaded if enabled, otherwise the snapshot. A command torn by a crash at the end of the AOF is truncated away; any other corruption stops the server from starting. If a background save or AOF write fails, writes are refused with `-MISCONF` until persistence recovers.

## Replication

Asynchronous master-replica replication using Redis's PSYNC protocol:

```sh
./build/src/kv_server --port 6380                                  # master
./build/src/kv_server --port 6381 --replicaof 127.0.0.1 6380       # replica
```

Or at runtime: `REPLICAOF host port`, and `REPLICAOF NO ONE` to promote a replica to master.

- **Full resync:** the master forks a snapshot, streams it to the replica, then sends the writes that happened meanwhile. The replica swaps in the new dataset, keeps the snapshot as its own, and restarts its AOF from it.
- **Streaming:** every write is sent to replicas as a deterministic command (the same form the AOF logs). Replicas are read-only (`-READONLY`), never expire or evict keys on their own (the master's `DEL`s drive that), and acknowledge their offset every second.
- **Partial resync:** the master keeps the recent stream in a circular backlog (`--repl-backlog-size`, default 1mb). A replica that reconnects after a short outage asks to continue from its offset and gets only the bytes it missed.
- **Failover:** a promoted replica keeps its old replication ID as a secondary one, so the other replicas of the old master can continue from it with a partial resync instead of a full copy.
- **Chaining:** a replica can have replicas of its own, which get the master's stream byte for byte.
- **`WAIT numreplicas timeout`** blocks a client until its writes are acknowledged by that many replicas. This narrows the window for losing an acknowledged write if the master crashes, but doesn't make replication synchronous or strongly consistent.
- `INFO replication` shows the role, link status, offsets, attached replicas and sync counters. Dead links are detected after `--repl-timeout` seconds (default 60).

## Cluster mode (sharding)

Data is split across several masters the way Redis Cluster does it, so standard cluster-aware Redis clients work unchanged:

```sh
for p in 7000 7001 7002; do mkdir -p data/$p; ./build/src/kv_server --port $p --dir data/$p --cluster-enabled yes & done
python3 tools/kv_cluster.py create 127.0.0.1:7000 127.0.0.1:7001 127.0.0.1:7002      # --replicas N for replicas
python3 tools/kv_cluster.py reshard --from 127.0.0.1:7000 --to 127.0.0.1:7001 --slots 1000
python3 tools/kv_cluster.py check 127.0.0.1:7000
```

- **Hash slots:** every key maps to one of 16384 slots (`CRC16(key) mod 16384`), and each master owns a set of slots. `{hash tags}` put related keys in the same slot, so multi-key commands work on them; multi-key commands across slots get `-CROSSSLOT`.
- **Redirects, not proxying:** a node asked about a key it doesn't own replies `-MOVED <slot> <ip>:<port>`, and the client updates its slot map. There is no extra hop on the data path.
- **Cluster bus:** nodes talk on a second port (client port + 10000, or `--cluster-port`). They PING each other and gossip their slots, config epochs, and a few other nodes they know, so one `CLUSTER MEET` joins a node to the whole cluster. When two nodes claim the same slot, the claim with the higher config epoch wins, and equal epochs are broken by node ID.
- **Live resharding:** slots move while the cluster serves traffic. The target marks the slot `IMPORTING`, the source `MIGRATING`, and keys move in batches with `MIGRATE` (atomic per batch). During the move, clients get `-ASK` for keys already transferred. `SETSLOT NODE` finishes the move, and the target takes a new config epoch so its claim spreads.
- **Replicas:** `CLUSTER REPLICATE <id>` sets up replication over the existing PSYNC stream. Replicas redirect writes to their master, and serve reads only on connections that sent `READONLY`.
- **State on disk:** each node keeps its ID, epochs and slot map in `nodes.conf` (in `--dir`), so it rejoins with the same identity after a restart.
- Options: `--cluster-node-timeout MS` (default 15000), `--cluster-config-file NAME`, `--cluster-require-full-coverage yes|no` (refuse key commands while any slot is unowned or its owner has failed; default yes).

## Failure detection and failover

When a master dies, one of its replicas takes over its slots automatically, as in Redis Cluster:

- **Suspicion (`PFAIL`):** a node that doesn't answer a PING within `--cluster-node-timeout` is flagged `fail?` by whoever noticed. Gossip spreads these suspicions, and when a master gossips a suspicion, it counts as a failure report.
- **Agreement (`FAIL`):** once a majority of the masters serving slots report the same node within twice the node timeout, it's flagged `fail`. A `FAIL` message makes every reachable node agree at once.
- **Election:** each replica of the failed master waits 0.5–1s, plus 1s for every sibling replica with a higher replication offset, so the most up-to-date replica usually goes first. It then increments the cluster's current epoch and asks every master for a vote. A master votes at most once per epoch and writes the vote to `nodes.conf` before sending it. A majority of votes wins.
- **Promotion:** the winner takes the failed master's slots under the new epoch. That epoch is higher than any other, so its claim wins on every node. The other replicas follow it, continuing their replication stream with a partial resync. When the old master comes back, it learns it was replaced and becomes a replica.
- **Safety limits:** a replica whose last contact with its master is older than `node_timeout × --cluster-replica-validity-factor` (default 10) plus the replication ping period won't try, because its data is too stale. `--cluster-replica-no-failover yes` disables automatic failover for a replica. A master that can't reach a majority of masters stops serving (`-CLUSTERDOWN`), so a partition's minority side can't keep accepting writes that the majority's failover would throw away.
- **Manual failover:** `CLUSTER FAILOVER` on a replica pauses writes on its master, waits until the replica has applied the master's final offset, then runs the election. No acknowledged write is lost, and clients' paused writes are redirected to the new master. `FORCE` skips the master (for when it's unreachable). `TAKEOVER` also skips the vote (for when a majority of masters is gone).

Replication stays asynchronous, so an automatic failover can still lose writes that the dead master acknowledged but never sent to its replica.

## Benchmarks

Linux/WSL, against the Release build:

```bash
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release && cmake --build build-release --target kv_server
python3 bench/bench.py baseline                                   # 1/4/16/64 clients, median of 3 -> bench/results/
python3 bench/bench.py run --clients 16 --pipeline 16             # one closed-loop run, 16-deep batches
python3 bench/bench.py run --clients 16 --rate 40000              # open loop: honest tail latency at 40k ops/s
python3 bench/bench.py crosscheck                                 # same load via redis-benchmark (needs redis-tools)
```

## Tests

```sh
(cd build && ctest)                             # unit tests (Catch2)
python3 tests/integration/test_server.py        # end-to-end tests against the real binary
python3 tests/integration/test_replication.py   # multi-process replication tests
python3 python-client/tests/test_cluster_client.py  # cluster client vs. scripted fake nodes
python3 tests/integration/test_cluster.py       # multi-process cluster tests (gossip, redirects, resharding, failover)
```
