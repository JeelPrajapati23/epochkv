# KV Store

A distributed key-value store (Redis-like) built from scratch in C++, with a Python client and benchmark harness.

## Features

- [x] Project setup (CMake, Catch2 test harness, minimal blocking TCP server)
- [ ] Core single-node store (RESP protocol parser, hash table, command dispatcher)
- [ ] Concurrency (epoll-based event loop)
- [ ] TTL & LRU eviction
- [ ] Persistence (AOF + snapshotting)
- [ ] Replication (master-replica)
- [ ] Sharding (consistent hashing)
- [ ] Fault tolerance (simplified leader election) — stretch goal
- [ ] Python client + benchmark harness (+ optional vector-search extension)
- [ ] Benchmarks and documentation
