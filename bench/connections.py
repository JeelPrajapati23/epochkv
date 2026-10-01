#!/usr/bin/env python3
"""Does serving cost depend on active connections, or on all of them?

  connections.py [--server kv|redis] [--idle 0 1000 5000 10000] [--rate 20000]

An event loop's claim is that idle connections are nearly free: epoll only
reports the sockets that have data, so 10,000 idle clients shouldn't slow
down the 16 that are talking. For each idle count this:
  1. opens that many idle connections from a separate process, each sending
     one PING so the server has fully set it up, and times how long that took;
  2. runs the usual open-loop load (bench.py run --rate) from 16 active
     clients, well below saturation, and records latency;
  3. records memory per connection, as the growth since the previous idle
     count: the server's RSS (our per-client structs and buffers), and the
     kernel's slab memory (socket structures, which RSS doesn't include;
     it counts both ends of each loopback connection). Growth between
     levels, not since startup: the preload's big pipeline buffers are freed
     and then reused for clients, which would hide the first ones' cost.
At the end every idle connection is PINGed again, to check none was dropped.

Both servers run with --maxclients 20000 (Redis's default is 10000, ours
too). Linux only; runs the Release build. Results go to bench/results/.
"""

import argparse
import multiprocessing as mp
import os
import resource
import socket
import sys
import time

import bench

PING = b"*1\r\n$4\r\nPING\r\n"
PONG = b"+PONG\r\n"


def read_exact(sock, n):
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise ConnectionError("closed")
        data += chunk
    return data


def holder(port, cpu, pipe):
    """Owns the idle connections. A separate process, so the load
    generator's forked workers don't inherit (and keep open) 10k sockets."""
    os.sched_setaffinity(0, {cpu})
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
    socks = []
    while True:
        cmd, arg = pipe.recv()
        if cmd == "grow":
            start = time.perf_counter()
            refused = 0
            while len(socks) < arg:
                s = socket.create_connection(("127.0.0.1", port))
                s.sendall(PING)
                if read_exact(s, len(PONG)) != PONG:
                    refused += 1
                    s.close()
                    continue
                socks.append(s)
            pipe.send((time.perf_counter() - start, refused))
        elif cmd == "ping_all":
            alive = 0
            for s in socks:
                try:
                    s.sendall(PING)
                    alive += read_exact(s, len(PONG)) == PONG
                except OSError:
                    pass
            pipe.send(alive)
        elif cmd == "close":
            for s in socks:
                s.close()
            pipe.send(None)
            return


def rss_kb(pid):
    with open("/proc/%d/status" % pid) as f:
        return next(int(line.split()[1]) for line in f if line.startswith("VmRSS"))


def slab_kb():
    with open("/proc/meminfo") as f:
        return next(int(line.split()[1]) for line in f if line.startswith("Slab:"))


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--server", choices=["kv", "redis"], default="kv")
    p.add_argument("--idle", type=int, nargs="+", default=[0, 1000, 5000, 10000])
    p.add_argument("--clients", type=int, default=16, help="active clients")
    p.add_argument("--rate", type=int, default=20000, help="open-loop ops/s, in total")
    p.add_argument("--duration", type=float, default=10)
    p.add_argument("--warmup", type=float, default=2)
    p.add_argument("--keys", type=int, default=100000)
    p.add_argument("--value-size", type=int, default=64)
    p.add_argument("--read-ratio", type=float, default=0.8)
    args = p.parse_args()
    if sorted(args.idle) != args.idle:
        sys.exit("--idle counts must be increasing: connections are only ever added")

    server_cpus, client_cpus = bench.cpu_layout()
    holder_cpu, client_cpus = client_cpus[-1], client_cpus[:-1]
    server = bench.Server(args.server, {server_cpus[0]}, extra=["--maxclients", "20000"])
    ctx = mp.get_context("fork")
    parent, child = ctx.Pipe()
    proc = ctx.Process(target=holder, args=(server.port, holder_cpu, child))
    proc.start()
    cfg = {"clients": args.clients, "workers": 0, "duration": args.duration, "warmup": args.warmup,
           "keys": args.keys, "value_size": args.value_size, "read_ratio": args.read_ratio,
           "pipeline": 1, "rate": args.rate, "client": "raw", "nodes": 1}
    runs = []
    try:
        bench.preload([server], args.keys, args.value_size)
        prev_idle, prev_rss, prev_slab = 0, rss_kb(server.proc.pid), slab_kb()
        for idle in args.idle:
            parent.send(("grow", idle))
            open_s, refused = parent.recv()
            if refused:
                sys.exit("%d connections were refused: is maxclients too low?" % refused)
            rss, slab = rss_kb(server.proc.pid), slab_kb()
            r = bench.run(cfg, [server], client_cpus)
            lat = r["latency_us"]
            run = {"idle": idle, "open_seconds": round(open_s, 3),
                   "server_rss_mb": round(rss / 1024, 1),
                   "rss_bytes_per_conn": round((rss - prev_rss) * 1024 / (idle - prev_idle)) if idle > prev_idle else None,
                   "kernel_slab_bytes_per_conn":
                       round((slab - prev_slab) * 1024 / (idle - prev_idle)) if idle > prev_idle else None,
                   "load": r}
            prev_idle, prev_rss, prev_slab = idle, rss, slab
            runs.append(run)
            print("%6d idle  opened in %5.2fs  RSS %6.1f MB (%s B/conn, kernel %s B/conn)   "
                  "%s ops/s  p50 %6.1fus  p99 %7.1fus  p99.9 %7.1fus  server CPU %5.1f%%" % (
                      idle, open_s, rss / 1024,
                      run["rss_bytes_per_conn"] if run["rss_bytes_per_conn"] is not None else "-",
                      run["kernel_slab_bytes_per_conn"] if run["kernel_slab_bytes_per_conn"] is not None else "-",
                      "{:,}".format(r["ops_per_sec"]), lat["p50"], lat["p99"], lat["p99.9"],
                      r["server_cpu_pct"]))
        parent.send(("ping_all", None))
        alive = parent.recv()
        print("idle connections still answering at the end: %d of %d" % (alive, args.idle[-1]))
        parent.send(("close", None))
        parent.recv()
    finally:
        if proc.is_alive():
            proc.terminate()
        proc.join()
        server.stop()
    meta = {"server": args.server, "commit": bench.git_commit(), "date": time.strftime("%Y-%m-%d %H:%M:%S"),
            "machine": bench.machine_info(), "keys": args.keys, "value_size": args.value_size,
            "read_ratio": args.read_ratio}
    bench.save("connections", {"meta": meta, "alive_at_end": alive, "runs": runs})


if __name__ == "__main__":
    main()
