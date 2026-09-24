# KV Store

A distributed key-value store (Redis-like) built from scratch in C++, with a Python client and benchmark harness.

## Features

- [x] Project setup (CMake, Catch2 test harness, minimal blocking TCP server)
- [x] Core single-node store (RESP protocol parser, hash table, command dispatcher)
- [x] Concurrency (epoll-based event loop)
- [x] TTL & LRU eviction
- [x] Persistence (AOF + snapshotting)
- [ ] Replication (master-replica)
- [ ] Sharding (consistent hashing)
- [ ] Fault tolerance (simplified leader election) — stretch goal
- [ ] Python client + benchmark harness (+ optional vector-search extension)
- [ ] Benchmarks and documentation

Supported commands: `PING`, `ECHO`, `SET` (with `EX`/`PX`/`EXAT`/`PXAT`/`NX`/`XX`/`KEEPTTL`), `GET`, `DEL`, `EXISTS`, `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `TTL`, `PTTL`, `PERSIST`, `DBSIZE`, `SAVE`, `BGSAVE`, `BGREWRITEAOF`, `LASTSAVE`.

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

## Tests

```sh
(cd build && ctest)                        # unit tests (Catch2)
python3 tests/integration/test_server.py   # end-to-end tests against the real binary
```
