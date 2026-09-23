# Design Log

## Phase 0: Blocking TCP Echo Server

**What was built:**
A single-threaded TCP server using raw POSIX sockets (`socket`/`bind`/`listen`/`accept`/`read`/`write`). It accepts exactly one client connection, echoes back whatever bytes it receives, and exits the loop on disconnect (`read` returning 0). No concurrency, no protocol framing — just enough to get comfortable with the socket API before Phase 1 adds RESP parsing.

**Alternative considered:**
Could have jumped straight to an epoll-based event loop (Phase 2's approach) or a thread-per-connection model, either of which would handle multiple clients. Skipped both for now.

**Why this approach:**
The point of Phase 0 is to build intuition for the raw socket calls themselves without the added complexity of concurrency. A blocking, single-connection server makes the limitation directly observable: with two `nc` clients connected, the second one gets no response because the process is parked inside `read()` for the first connection — `accept()` is never reached again. That's a concrete, hands-on motivation for Phase 2's epoll event loop, rather than an abstract argument for why blocking I/O doesn't scale.
