# EpochKV

**A Redis-compatible distributed key-value store, built from scratch in C++17.**

EpochKV is a sharded, replicated key-value store with automatic failover. It speaks the Redis protocol (RESP), so `redis-cli`, `redis-benchmark` and cluster-aware Redis clients work with it unchanged. It includes a cluster-aware Python client and a benchmark harness.

A single node is a single-threaded epoll server with key expiry, LRU eviction, and persistence through fork-based snapshots plus an append-only file. Nodes can be chained into master-replica replication with partial resync. A cluster shards data over 16384 hash slots, coordinates over a gossip bus, moves slots live while serving traffic, and fails over automatically when a master dies.

### Demo: killing a master under live writes

[![EpochKV failover demo](https://asciinema.org/a/AlZAYMDTe66dcSTJ.svg)](https://asciinema.org/a/AlZAYMDTe66dcSTJ)

A 3-master, 3-replica cluster takes writes through the Python client while one master is killed with `kill -9`. The survivors suspect it (PFAIL), agree by majority (FAIL), and its replica wins an election and takes over its slots about 3 seconds after the kill. The script then checks every acknowledged write, and restarts the old master, which rejoins as a replica. Run it yourself with `python3 tools/demo_failover.py`.

**Headline numbers** (i7-13650HX, WSL2, loopback, 80/20 GET/SET, 64-byte values; see [Benchmarks](#benchmarks)):

| 1 node, ops/s (median of 3) | EpochKV | Redis 8.0.5 |
|---|---|---|
| 16 clients, no pipelining | 51.1k | 49.4k |
| 12 clients × 64-deep pipelines (Python client) | **941k** | 864k |
| `redis-benchmark`, 16 clients × 64, GET | 1.08M | 1.12M |
| `redis-benchmark`, 16 clients × 64, SET | **941k** | 990k |

Each row compares both servers in the same session, with their runs interleaved; compare within a row, not across rows. The pipelined rows ran in a session where the machine was slower overall, and Redis's harness runs varied from 774k to 888k, so that row is parity, not a win. A 3-node cluster reached **1.23M** ops/s with 12 clients × 64-deep pipelines (Python client, p99 983µs). That number is limited by the load generator, so it's a lower bound.

Pipelining is about 18× faster on one node because it removes a wakeup per request, which measurement showed to be the main cost. With deep pipelines our own code becomes most of the CPU time, which exposed a gap: Redis was **1.47× faster on SET**. An experiment that disabled propagation recovered 40% of SET throughput, so propagation was the cause: every SET was copied, RESP-encoded and appended to the replication backlog even with no replicas and no AOF. Now, as in Redis, the replication stream starts only when a replica first connects, and writes skip building their logged command while nothing reads it. The SET gap is down to ~5%.

## Architecture

```
                    clients (redis-cli, kvclient, any RESP client)
                                    │  RESP over TCP
┌───────────────────────────────────▼──────────────────────────────────────┐
│ kv_server: one thread, one epoll loop                                    │
│                                                                          │
│  socket read ─► incremental RESP parser ─► command table (arity, flags)  │
│                                                  │                       │
│                  cluster check: slot owner? ─────┤── no ─► -MOVED / -ASK │
│                                                  ▼                       │
│              Store: chained hash table + exact LRU list + TTL index      │
│                    (lazy + sampled active expiry, maxmemory eviction)    │
│                                                  │                       │
│       ┌──────────────────────┬───────────────────┼──────────────────┐    │
│       ▼                      ▼                   ▼                  │    │
│  reply buffer ─► send   AOF (multi-part)   replication stream       │    │
│                        fsync per policy   + circular backlog        │    │
│                                                  │                  │    │
│  cron: active expiry, save points, replica acks, cluster timers ◄───┘    │
│  fork() child: snapshot / AOF rewrite / full-resync payload (CoW)        │
└──────────────┬────────────────────────────────────┬──────────────────────┘
               │ PSYNC                              │ cluster bus (port + 10000)
               ▼                                    ▼
           replicas                        other nodes: PING/PONG gossip,
     (read-only, apply stream)             FAIL, failover votes, epochs
```

## Design decisions

The main choices, and why. Each one was measured or argued against an alternative.

- **Single-threaded, level-triggered epoll loop instead of a thread per connection.** Only one thread touches the store, so there are no locks, and a single thread can serve thousands of mostly idle connections. The cost is that commands use one core and a slow command stalls everyone. That's acceptable because in-memory commands take about 2µs and benchmarks showed most time goes to the kernel, not our code. Level-triggered mode (one 16KB read per wakeup) avoids edge-triggered's "missed one drain, client stuck forever" bug and keeps connections fair.
- **Incremental RESP parser with hand-written validation.** It handles any split of bytes across reads, so the network can deliver partial commands safely. Lengths are checked digit by digit rather than with `stoul`, because `stoul` silently accepts input like `-1` and `12abc`, and this input comes from untrusted clients.
- **Replies appended to a per-connection output buffer.** A pipelined batch of 100 commands goes out in one `send()` instead of 100. This is what makes pipelining cheap on the server side.
- **Separate-chaining hash table with FNV-1a, plus an exact LRU (hash map + linked list).** Chaining makes deletion simple (no tombstones) and handles high load factors well. LRU is exact and O(1) (`splice` moves a node without allocating), whereas Redis samples keys to save 16 bytes per key. That's the right trade at hundreds of millions of keys, not here.
- **Incremental rehashing over linked-list chains.** A resize moves one bucket per write (plus a 1 ms slice per 100 ms tick), and every lookup still checks exactly one bucket. Each bucket is an 8-byte head pointer, and the new bucket array comes from `calloc`, which gets already-zeroed pages from the OS instead of writing every bucket up front. So no step of a resize grows with the table: the worst pause while loading 5M keys went from ~1 s (rehash all at once) to 124 ms (incremental, `std::vector` buckets) to 8.9 ms, and per-key memory fell by 35–68 B.
- **Expiry: lazy on access plus sampled active cleanup instead of a timer per key.** A key that outlives its TTL is invisible anyway, so cleanup doesn't have to be exact. Twenty random samples per round, with another round when more than 25% have expired, bound both memory held by dead keys and CPU spent collecting them. Deadlines use the wall clock, because they're written to disk; timers use a monotonic clock, so an NTP clock jump can't make them misfire.
- **Snapshots and AOF together, both written by a `fork()`ed child.** Snapshots load fast but lose minutes of writes in a crash. The AOF loses at most one second but grows without limit. Using a snapshot as the AOF's base gets both benefits. `fork()` gives a consistent point-in-time copy for free through copy-on-write, and the multi-part AOF layout switches new writes to a fresh file *before* forking, so no rewrite buffer ever needs merging. Failed persistence refuses writes (`-MISCONF`) instead of losing them silently.
- **Asynchronous replication with PSYNC and a circular backlog.** A replica that briefly disconnects resumes from its offset instead of re-copying the whole dataset. Replicas never expire keys on their own: the master's DELs drive that, so replicas can't diverge. `WAIT` narrows the window for losing acknowledged writes but is explicitly *not* strong consistency.
- **Hash slots instead of a consistent-hashing ring.** Both move about 1/N of keys when membership changes. With slots, ownership is an explicit 16384-entry table: easy to inspect, cheap to gossip as a 2KB bitmap, and movable one slot at a time. That's what makes live resharding manageable. Clients are redirected (`MOVED`/`ASK`) rather than proxied, so stale routing costs one extra round trip instead of an extra hop on every request.
- **Gossip plus config epochs instead of a central coordinator (ZooKeeper/etcd).** There's no extra system to run. Conflicting slot claims resolve deterministically: the higher epoch wins, and node ID breaks ties.
- **Failover by majority vote, with split-brain guards.** A node is marked failed only when a majority of masters agree. A master votes at most once per epoch and saves the vote to disk before sending it. The most up-to-date replica tends to run first, because each replica waits longer the further it is behind. A master that can't reach a majority stops accepting writes, so a minority partition can't collect writes that the majority's failover would discard.
- **Client retries are at-most-once.** Reads and redirected commands are retried. A write whose connection died without a reply raises `UncertainWriteError` instead of being re-sent, because re-sending could apply it twice. Inside a pipeline, a read isn't retried if an unacknowledged write to the same slot comes after it; otherwise the read could observe that later write.
- **Measure before optimizing.** The benchmark harness has an open-loop mode to avoid coordinated omission, and a mergeable log-linear latency histogram with ≤3% error. It showed that server CPU per request tracked how often a reply had to wake a sleeping client, and that's what justified building pipelining.

**Known limitations:**
- There are no transactions (`MULTI`/`EXEC`), so a pipeline isn't atomic.
- Replication is asynchronous, so an automatic failover can lose writes the old master acknowledged but never sent to its replica.
- The 3-node benchmark numbers are limited by the Python load generator, not by the cluster.

## Features

- Supports strings (including integer counters), TTLs and eviction, plus snapshots and an AOF.
- Replication supports full and partial resync, chained replicas and `WAIT`.
- Cluster mode supports hash slots, gossip, `MOVED`/`ASK`, live resharding, automatic failover and manual `CLUSTER FAILOVER`.
- The Python client (`python-client/kvclient`) is cluster-aware and supports pipelining across nodes.
- The benchmark harness (`bench/`) supports closed- and open-loop load, comparison with Redis, and runs through the real client.

Supported commands: `PING`, `ECHO`, `SET` (with `EX`/`PX`/`EXAT`/`PXAT`/`NX`/`XX`/`KEEPTTL`), `GET`, `DEL`, `EXISTS`, `INCR`, `DECR`, `INCRBY`, `DECRBY`, `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `TTL`, `PTTL`, `PERSIST`, `DBSIZE`, `SAVE`, `BGSAVE`, `BGREWRITEAOF`, `LASTSAVE`, `REPLICAOF`, `WAIT`, `INFO`, `DUMP`, `RESTORE`, `MIGRATE`, `CLUSTER` (`INFO`, `NODES`, `SLOTS`, `MYID`, `MEET`, `ADDSLOTS[RANGE]`, `DELSLOTS[RANGE]`, `SETSLOT`, `KEYSLOT`, `COUNTKEYSINSLOT`, `GETKEYSINSLOT`, `REPLICATE`, `FORGET`, `FAILOVER`, `COUNT-FAILURE-REPORTS`, `SET-CONFIG-EPOCH`, `BUMPEPOCH`, `SAVECONFIG`), `ASKING`, `READONLY`, `READWRITE`.

## Build and run

Requires Linux (epoll), CMake 3.20+, and a C++17 compiler.

```sh
cmake -S . -B build
cmake --build build -j
./build/src/kv_server                  # listens on 127.0.0.1:6380
./build/src/kv_server --bind 0.0.0.0 --port 7000
./build/src/kv_server --maxmemory 100mb --maxmemory-policy allkeys-lru
./build/src/kv_server --maxclients 20000   # default 10000; raises the open-file limit to fit
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
- **Safety limits:** a replica whose last contact with its master is older than `node_timeout × --cluster-replica-validity-factor` (default 10) plus the replication ping period won't try, because its data is too stale. `--cluster-replica-no-failover yes` disables automatic failover for a replica. A master that can't reach a majority of masters stops serving (`-CLUSTERDOWN`), so a partition's minority side can't keep accepting writes that the majority's failover would throw away. The same applies after a freeze: a master whose event loop didn't run for more than half the node timeout (a `SIGSTOP`, a stalled VM) refuses commands on waking until gossip has refreshed its view, instead of serving the writes that queued up meanwhile as if it were still master (see below).
- **Manual failover:** `CLUSTER FAILOVER` on a replica pauses writes on its master, waits until the replica has applied the master's final offset, then runs the election. No acknowledged write is lost, and clients' paused writes are redirected to the new master. `FORCE` skips the master (for when it's unreachable). `TAKEOVER` also skips the vote (for when a majority of masters is gone).

Replication stays asynchronous, so an automatic failover can still lose writes that the dead master acknowledged but never sent to its replica.

## Benchmarks

Linux/WSL, against the Release build:

```bash
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release && cmake --build build-release --target kv_server
python3 bench/bench.py baseline                                   # 1/4/16/64 clients, median of 3 -> bench/results/
python3 bench/bench.py run --clients 16 --pipeline 16             # one closed-loop run, 16-deep batches
python3 bench/bench.py run --clients 16 --rate 40000              # open loop: honest tail latency at 40k ops/s
python3 bench/bench.py run --client kvclient --clients 12 --pipeline 16 --nodes 3   # real client, 3-master cluster
python3 bench/bench.py crosscheck                                 # same load via redis-benchmark (needs redis-tools)
python3 bench/bench.py crosscheck --pipeline 64                   # ... with 64-deep pipelines (redis-benchmark -P)
python3 bench/connections.py --server kv                          # latency and memory with 0..10k idle connections
```

### Idle connections

`bench/connections.py` holds 0 to 10,000 idle connections open while 16 active clients run a fixed 20k ops/s:

| idle connections | 0 | 1,000 | 5,000 | 10,000 |
|---|---|---|---|---|
| EpochKV p50 / p99 | 172 / 352µs | 172 / 360µs | 168 / 360µs | 164 / 426µs |
| EpochKV server CPU | 52% | 51% | 53% | 53% |

Latency and CPU stay flat: epoll's cost depends on active connections, not open ones (Redis 8 measured the same). Each connection costs ~250 B of server memory (Redis: ~2.5–5 KB) and ~4 KB per socket in the kernel, which dominates.

## Network emulation and fault injection

`bench/docker/` runs six nodes and a client as separate containers, each with its own IP, a CPU and 512 MB cap per node, and `tc netem` shaping each container's outgoing traffic (delay, jitter, loss).

```bash
docker compose -f bench/docker/compose.yml up -d --build
python3 bench/netem.py latency                    # throughput vs round-trip time
python3 bench/netem.py failover --mode kill       # kill -9 a master, under 4 network profiles
python3 bench/netem.py failover --mode pause      # freeze a master past its failover, then thaw it
docker compose -f bench/docker/compose.yml down
```

**Latency caps throughput** (ops/s, one server container, closed loop):

| added RTT | 0 | 0.5 ms | 1 ms | 2 ms | 5 ms |
|---|---|---|---|---|---|
| 1 client | 13,067 | 1,438 | 776 | 414 | 183 |
| 16 clients | 43,831 | 22,534 | 12,999 | 6,780 | 2,892 |
| 16 clients, pipelined ×16 | 448,426 | 316,525 | 203,504 | 107,296 | 45,395 |

Throughput is requests in flight divided by the round trip (Little's Law): one client gets ~1/RTT, and pipelining multiplies what's in flight.

**Failover under realistic networks** (2s node timeout, a writer running throughout; profiles: none, 2 ms RTT, plus 1% loss, 5 ms RTT with jitter and 5% loss):
- `kill -9` of a master: the replica took over in 3.1–3.4 s under every profile, and no acknowledged write was lost.
- No healthy node was ever falsely suspected, even at 5% packet loss: TCP retransmits within ~200 ms, far inside the timeout. Only silence longer than the timeout (a partition) looks like a failure.
- **Freezing a master (`docker pause`) found a bug.** On waking, the old master served the client writes that had queued in its sockets while it was frozen, as if still master, acknowledged them, learned a millisecond later that it had been replaced, and lost them when it resynced. That lost one acknowledged write in every run. The fix: the event loop notices it didn't run for more than half the node timeout and refuses commands (`-CLUSTERDOWN`) until gossip has refreshed its view. After the fix: no acknowledged write lost, and the first reply after waking is `CLUSTERDOWN` under every profile.

## Tests

```sh
(cd build && ctest)                             # unit tests (Catch2)
python3 tests/integration/test_server.py        # end-to-end tests against the real binary
python3 tests/integration/test_replication.py   # multi-process replication tests
python3 python-client/tests/test_cluster_client.py  # cluster client vs. scripted fake nodes
python3 tests/integration/test_cluster.py       # multi-process cluster tests (gossip, redirects, resharding, failover)
```
