# KV Store

A distributed key-value store (Redis-like) built from scratch in C++, with a Python client and benchmark harness.

## Phases

- [x] Phase 0: Setup (CMake, git, test framework, minimal blocking TCP server)
- [ ] Phase 1: Core single-node store (RESP protocol parser, hash table, command dispatcher)
- [ ] Phase 2: Concurrency (epoll-based event loop)
- [ ] Phase 3: TTL & LRU eviction
- [ ] Phase 4: Persistence (AOF + snapshotting)
- [ ] Phase 5: Replication (master-replica)
- [ ] Phase 6: Sharding (consistent hashing)
- [ ] Phase 7 (stretch): Fault tolerance (simplified leader election)
- [ ] Phase 8: Python client + benchmark harness (+ optional vector-search extension)
- [ ] Phase 9: Resume/interview polish (DESIGN.md, README, benchmarks)
