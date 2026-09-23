# KV Store

A distributed key-value store (Redis-like) built from scratch in C++, with a Python client and benchmark harness.

## Features

- [x] Project setup (CMake, Catch2 test harness, minimal blocking TCP server)
- [x] Core single-node store (RESP protocol parser, hash table, command dispatcher)
- [x] Concurrency (epoll-based event loop)
- [x] TTL & LRU eviction
- [ ] Persistence (AOF + snapshotting)
- [ ] Replication (master-replica)
- [ ] Sharding (consistent hashing)
- [ ] Fault tolerance (simplified leader election) — stretch goal
- [ ] Python client + benchmark harness (+ optional vector-search extension)
- [ ] Benchmarks and documentation

Supported commands: `PING`, `ECHO`, `SET` (with `EX`/`PX`/`NX`/`XX`/`KEEPTTL`), `GET`, `DEL`, `EXISTS`, `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST`, `DBSIZE`.

## Build and run

Requires Linux (epoll), CMake 3.20+, and a C++17 compiler.

```sh
cmake -S . -B build
cmake --build build -j
./build/src/kv_server                  # listens on 127.0.0.1:6380
./build/src/kv_server --bind 0.0.0.0 --port 7000
./build/src/kv_server --maxmemory 100mb --maxmemory-policy allkeys-lru
```

Any Redis client works, e.g. `redis-cli -p 6380`.

## Tests

```sh
(cd build && ctest)                        # unit tests (Catch2)
python3 tests/integration/test_server.py   # end-to-end tests against the real binary
```
