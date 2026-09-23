# CLAUDE.md — KV Store Project (Redis Clone)

## Project Context
Building a distributed key-value store (Redis-like) in C++ with a Python client/benchmark harness, as a portfolio project for SDE/backend placement interviews. The goal is genuine understanding of distributed-systems concepts (networking, concurrency, replication, sharding, fault tolerance) — not just a working repo. I need to be able to defend every design decision live, including in on-the-spot coding rounds.

## Core Working Rules

1. **Explain before implementing.** For any new component, first explain the concept, the trade-offs, and the alternatives considered. Do NOT write implementation code until I've confirmed I understand the approach.

2. **Keep implementation chunks small.** Don't generate more than ~15 lines of core logic at once without pausing for me to review and confirm understanding.

3. **Flag non-obvious decisions.** Whenever there's a real trade-off (e.g. epoll vs thread-per-connection, AOF vs snapshotting, consistent hashing vs range partitioning), explicitly call it out and explain the reasoning — don't silently pick one and move on.

4. **Boilerplate vs core logic — different rules:**
   - Scaffolding, build config (CMakeLists.txt), test harness setup, the Python client/benchmark tooling → fine to generate directly, low conceptual stakes.
   - Core logic (hash table, LRU eviction, event loop, protocol parsing, replication, sharding, fault tolerance) → I write this myself. Use **review mode**: point out bugs, edge cases, and improvements in code I've already written, rather than replacing it with your own implementation.

5. **After each component, prompt me to log it.** A short DESIGN.md entry: what was built, what alternative was considered, why this approach was chosen. If I can't articulate it, that's a signal to slow down, not move forward.

6. **Don't assume systems background.** Treat networking (epoll/sockets), the Redis protocol, persistence, replication, and consensus/fault tolerance as genuinely new territory for me — explain from first principles rather than assuming familiarity.

7. **DSA-adjacent pieces can move faster.** Hash tables, LRU cache logic, and consistent-hashing math build directly on my competitive programming background — less discussion needed here, more direct implementation with light review.

8. **Git commit/push is user-only.** I run `git add`/`commit`/`push` myself. Claude should never execute these — only give me the exact commands to run.

## Roadmap Reference
- Phase 0: Setup (CMake, git, test framework, minimal blocking TCP server)
- Phase 1: Core single-node store (RESP protocol parser, hash table, command dispatcher)
- Phase 2: Concurrency (epoll-based event loop)
- Phase 3: TTL & LRU eviction
- Phase 4: Persistence (AOF + snapshotting)
- Phase 5: Replication (master-replica)
- Phase 6: Sharding (consistent hashing)
- Phase 7 (stretch): Fault tolerance (simplified leader election)
- Phase 8: Python client + benchmark harness (+ optional vector-search extension)
- Phase 9: Resume/interview polish (DESIGN.md, README, benchmarks)
