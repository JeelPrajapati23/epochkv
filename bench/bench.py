#!/usr/bin/env python3
"""Load generator for kv_server, with real Redis as an optional reference.

  bench.py run [--server kv|redis] [--clients N] [--rate OPS] [--pipeline P] ...
      One measurement. Closed loop by default: each connection sends a
      request (or a batch of --pipeline requests) and waits for the replies
      before sending more; this finds peak throughput. With --rate it's open
      loop: requests go out on a fixed schedule whether or not replies are
      back, and latency counts from the *scheduled* send time, so server
      stalls show up in full (no coordinated omission).
  bench.py baseline [--server kv|redis]
      The baseline matrix: closed loop at 1/4/16/64 clients, median of 3
      runs each, written to bench/results/.
  bench.py crosscheck
      The same client counts through redis-benchmark (a C load generator),
      to check the Python harness isn't what's being measured.

Linux only (sched_setaffinity, /proc). Runs the Release build.
"""

import argparse
import collections
import json
import multiprocessing as mp
import os
import platform
import random
import selectors
import shutil
import socket
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(ROOT, "python-client"))
from histogram import Histogram  # noqa: E402
from kvclient.resp import Conn, encode  # noqa: E402

KV_BIN = os.path.join(ROOT, "build-release", "src", "kv_server")
RESULTS = os.path.join(HERE, "results")


# -- machine -----------------------------------------------------------------

def cpu_layout():
    """(server cpus, client cpus): the server gets one CPU to itself, and its
    hyperthread sibling stays idle so no client shares that physical core."""
    cpus = sorted(os.sched_getaffinity(0))
    server = cpus[0]
    try:
        with open("/sys/devices/system/cpu/cpu%d/topology/thread_siblings_list" % server) as f:
            siblings = parse_cpu_list(f.read())
    except OSError:
        siblings = {server}
    return {server}, [c for c in cpus if c not in siblings]


def parse_cpu_list(s):
    out = set()
    for part in s.strip().split(","):
        lo, _, hi = part.partition("-")
        out.update(range(int(lo), int(hi or lo) + 1))
    return out


def cpu_seconds(pid):
    """(user, system) CPU seconds a process has used so far. System time
    includes the kernel's TCP work: on loopback, sending a packet runs the
    receive path too, charged to the sender."""
    with open("/proc/%d/stat" % pid) as f:
        fields = f.read().rsplit(")", 1)[1].split()  # the name can contain spaces
    tick = os.sysconf("SC_CLK_TCK")
    return int(fields[11]) / tick, int(fields[12]) / tick


def sleeps(pid):
    """How many times a process has blocked waiting (voluntary context
    switches): for the server, roughly how often epoll_wait found no work."""
    with open("/proc/%d/status" % pid) as f:
        return next(int(line.split()[1]) for line in f if line.startswith("voluntary_ctxt_switches"))


def git_commit():
    try:
        return subprocess.run(["git", "-C", ROOT, "rev-parse", "--short", "HEAD"],
                              capture_output=True, text=True, check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def machine_info():
    model = ""
    try:
        with open("/proc/cpuinfo") as f:
            model = next(line.split(":", 1)[1].strip() for line in f if line.startswith("model name"))
    except (OSError, StopIteration):
        pass
    return {"cpu": model, "cpus": os.cpu_count(), "kernel": platform.release(),
            "python": platform.python_version()}


# -- server ------------------------------------------------------------------

class Server:
    """A server process pinned to its own CPU, with persistence off so the
    baseline measures the request path only."""

    def __init__(self, kind, cpus, port=None):
        self.kind = kind
        self.port = port or free_port()
        self.dir = tempfile.mkdtemp(prefix="kvbench-")
        if kind == "kv":
            if not os.path.exists(KV_BIN):
                sys.exit("no Release build at %s; run: cmake -S . -B build-release "
                         "-DCMAKE_BUILD_TYPE=Release && cmake --build build-release" % KV_BIN)
            cmd = [KV_BIN, "--port", str(self.port), "--dir", self.dir, "--save", ""]
        elif kind == "redis":
            if not shutil.which("redis-server"):
                sys.exit("redis-server not installed (sudo apt-get install redis-server)")
            cmd = ["redis-server", "--port", str(self.port), "--dir", self.dir, "--save", "",
                   "--appendonly", "no", "--protected-mode", "no", "--daemonize", "no"]
        else:
            raise ValueError(kind)
        self.proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        os.sched_setaffinity(self.proc.pid, cpus)
        deadline = time.time() + 10
        while True:
            try:
                c = Conn("127.0.0.1", self.port, timeout=1)
                c.call("PING")
                c.close()
                return
            except OSError:
                if time.time() > deadline or self.proc.poll() is not None:
                    raise RuntimeError("%s didn't start" % kind)
                time.sleep(0.05)

    def stop(self):
        self.proc.terminate()
        self.proc.wait(10)
        shutil.rmtree(self.dir, ignore_errors=True)


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def preload(port, keys, value_size):
    """Creates every key once, so GETs hit and memory is in steady state."""
    c = Conn("127.0.0.1", port, timeout=30)
    value = b"x" * value_size
    for start in range(0, keys, 1000):
        batch = [("SET", "key:%d" % i, value) for i in range(start, min(start + 1000, keys))]
        c.send_many(batch)
        for _ in batch:
            c.read_reply()
    c.close()


# -- the load generator --------------------------------------------------------

def request_pool(cfg, seed, size=10000):
    """Pre-encoded requests, cycled through: encoding in the hot loop would
    add client time to every measurement. redis-benchmark does the same."""
    rng = random.Random(seed)
    value = b"x" * cfg["value_size"]
    pool = []
    for _ in range(size):
        key = "key:%d" % rng.randrange(cfg["keys"])
        pool.append(encode("GET", key) if rng.random() < cfg["read_ratio"] else encode("SET", key, value))
    return pool


def reply_end(buf, pos):
    """Offset just past the complete reply starting at buf[pos], or -1."""
    eol = buf.find(b"\r\n", pos)
    if eol == -1:
        return -1
    kind = buf[pos]
    if kind == 0x24:  # '$' bulk string
        n = int(buf[pos + 1:eol])
        if n < 0:
            return eol + 2
        end = eol + 2 + n + 2
        return end if end <= len(buf) else -1
    if kind == 0x2A:  # '*' array
        p = eol + 2
        for _ in range(max(int(buf[pos + 1:eol]), 0)):
            p = reply_end(buf, p)
            if p == -1:
                return -1
        return p
    return eol + 2  # '+', '-', ':' are one line


class Link:
    """One connection's state. `inflight` holds, in send order, the time
    each outstanding request counts from: when it was sent (closed loop) or
    when it was scheduled (open loop). Replies come back in the same order."""

    def __init__(self, port):
        self.sock = socket.create_connection(("127.0.0.1", port))
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.buf = bytearray()
        self.inflight = collections.deque()
        self.next_due = 0


def worker(cfg, port, n_links, seed, cpus, barrier, results):
    os.sched_setaffinity(0, cpus)
    pool = request_pool(cfg, seed)
    pool_i = 0
    links = [Link(port) for _ in range(n_links)]
    sel = selectors.DefaultSelector()
    for link in links:
        sel.register(link.sock, selectors.EVENT_READ, link)
    hist, lag = Histogram(), Histogram()
    ops = errors = 0
    pipeline, rate = cfg["pipeline"], cfg["rate"]
    interval = cfg["clients"] * 1e9 / rate if rate else 0  # ns between one link's requests
    now_ns = time.perf_counter_ns

    def send(link, stamps):
        """One write carrying a request per stamp."""
        nonlocal pool_i
        out = []
        for stamp in stamps:
            out.append(pool[pool_i])
            pool_i = (pool_i + 1) % len(pool)
            link.inflight.append(stamp)
        link.sock.sendall(b"".join(out))  # blocking: requests are tiny

    barrier.wait()
    start = now_ns()
    measure_from = start + int(cfg["warmup"] * 1e9)
    end = measure_from + int(cfg["duration"] * 1e9)
    cpu_at_start = None
    sleeps_at_start = 0
    if rate:
        for i, link in enumerate(links):  # stagger the links over one interval
            link.next_due = start + int(interval * i / n_links)
    else:
        for link in links:
            send(link, [start] * pipeline)

    while True:
        now = now_ns()
        if now >= end:
            break
        if cpu_at_start is None and now >= measure_from:
            t = os.times()
            cpu_at_start = t.user + t.system
            sleeps_at_start = sleeps(os.getpid())
        if rate:
            timeout = max(0, min(link.next_due for link in links) - now) / 1e9
        else:
            timeout = 0.1
        for key, _ in sel.select(timeout):
            link = key.data
            data = link.sock.recv(65536)
            if not data:
                raise ConnectionError("server closed a benchmark connection")
            link.buf += data
            now = now_ns()
            pos = 0
            while True:
                nxt = reply_end(link.buf, pos)
                if nxt == -1:
                    break
                if link.buf[pos] == 0x2D:  # '-': an error reply
                    errors += 1
                sent = link.inflight.popleft()
                if now >= measure_from:
                    hist.record(now - sent)
                    ops += 1
                pos = nxt
            del link.buf[:pos]
            if not rate and not link.inflight:
                send(link, [now] * pipeline)  # closed loop: next batch once all replies are in
        if rate:
            now = now_ns()
            for link in links:
                stamps = []
                while link.next_due <= now:
                    stamps.append(int(link.next_due))
                    link.next_due += interval
                if stamps:
                    # Stamped with the *scheduled* times, however late they
                    # actually go out: lateness is part of the latency.
                    for stamp in stamps:
                        if stamp >= measure_from:
                            lag.record(now - stamp)
                    send(link, stamps)

    t = os.times()
    cpu = t.user + t.system - (cpu_at_start if cpu_at_start is not None else t.user + t.system)
    for link in links:
        link.sock.close()
    results.put({"hist": hist.state(), "lag": lag.state(), "ops": ops, "errors": errors, "cpu": cpu,
                 "sleeps": sleeps(os.getpid()) - sleeps_at_start})


def run(cfg, server, client_cpus):
    """One measurement against a running, preloaded server."""
    workers = min(cfg["clients"], len(client_cpus)) if cfg["workers"] == 0 else cfg["workers"]
    cfg = dict(cfg, workers=workers)
    ctx = mp.get_context("fork")
    barrier = ctx.Barrier(workers + 1)
    results = ctx.Queue()
    procs = []
    for w in range(workers):
        # Spread the connections as evenly as possible over the workers.
        n = cfg["clients"] // workers + (1 if w < cfg["clients"] % workers else 0)
        cpu = {client_cpus[w % len(client_cpus)]}
        p = ctx.Process(target=worker, args=(cfg, server.port, n, 1000 + w, cpu, barrier, results))
        p.start()
        procs.append(p)
    barrier.wait()
    time.sleep(cfg["warmup"])
    user0, sys0 = cpu_seconds(server.proc.pid)
    sleeps0 = sleeps(server.proc.pid)
    time.sleep(cfg["duration"])
    user1, sys1 = cpu_seconds(server.proc.pid)
    sleeps1 = sleeps(server.proc.pid)
    parts = [results.get(timeout=60) for _ in procs]
    for p in procs:
        p.join()

    hist, lag = Histogram(), Histogram()
    for part in parts:
        hist.merge(Histogram.from_state(part["hist"]))
        lag.merge(Histogram.from_state(part["lag"]))
    us = lambda ns: round(ns / 1000, 1)  # noqa: E731
    result = {
        "config": cfg,
        "ops_per_sec": round(sum(p["ops"] for p in parts) / cfg["duration"]),
        "latency_us": {"mean": us(hist.mean()), "p50": us(hist.percentile(50)), "p90": us(hist.percentile(90)),
                       "p99": us(hist.percentile(99)), "p99.9": us(hist.percentile(99.9)), "max": us(hist.max)},
        "server_cpu_pct": round(100 * (user1 - user0 + sys1 - sys0) / cfg["duration"], 1),
        "server_user_pct": round(100 * (user1 - user0) / cfg["duration"], 1),
        "server_sys_pct": round(100 * (sys1 - sys0) / cfg["duration"], 1),
        "server_sleeps_per_op": None,
        "client_cpu_pct_max": round(100 * max(p["cpu"] for p in parts) / cfg["duration"], 1),
        "errors": sum(p["errors"] for p in parts),
    }
    ops = result["ops_per_sec"] * cfg["duration"]
    result["server_sleeps_per_op"] = round((sleeps1 - sleeps0) / ops, 3) if ops else None
    # Each client sleep ends with a wakeup, which the server's write() pays for.
    result["client_sleeps_per_op"] = round(sum(p["sleeps"] for p in parts) / ops, 3) if ops else None
    if cfg["rate"]:
        result["send_lag_us"] = {"p99": us(lag.percentile(99)), "max": us(lag.max)}
    return result


# -- reporting ---------------------------------------------------------------

def describe(r):
    lat = r["latency_us"]
    cfg = r["config"]
    shape = "%4d clients" % cfg["clients"] + (" x%-3d" % cfg["pipeline"] if cfg["pipeline"] > 1 else "     ")
    line = ("%s %9s ops/s   p50 %7.1fus  p99 %7.1fus  p99.9 %8.1fus   "
            "server CPU %5.1f%% (user %4.1f%% sys %4.1f%%)  sleeps/op %5.3f  client CPU %5.1f%% sleeps/op %5.3f") % (
        shape, "{:,}".format(r["ops_per_sec"]), lat["p50"], lat["p99"], lat["p99.9"],
        r["server_cpu_pct"], r["server_user_pct"], r["server_sys_pct"], r["server_sleeps_per_op"] or 0,
        r["client_cpu_pct_max"], r["client_sleeps_per_op"] or 0)
    warnings = []
    if r["client_cpu_pct_max"] > 90:
        warnings.append("client-bound: a worker is near 100% CPU, so this may measure Python, not the server")
    if r["errors"]:
        warnings.append("%d error replies" % r["errors"])
    if "send_lag_us" in r and r["send_lag_us"]["p99"] > 1000:
        warnings.append("client fell behind its schedule (send lag p99 %.0fus)" % r["send_lag_us"]["p99"])
    return line + "".join("\n      ! " + w for w in warnings)


def save(name, doc):
    os.makedirs(RESULTS, exist_ok=True)
    path = os.path.join(RESULTS, "%s-%s-%s.json" % (name, doc["meta"]["server"], doc["meta"]["commit"]))
    with open(path, "w") as f:
        json.dump(doc, f, indent=2)
    print("wrote %s" % os.path.relpath(path, ROOT))


def meta(args):
    return {"server": args.server, "commit": git_commit(), "date": time.strftime("%Y-%m-%d %H:%M:%S"),
            "machine": machine_info(), "keys": args.keys, "value_size": args.value_size,
            "read_ratio": args.read_ratio}


def config(args, clients, rate=0, pipeline=1):
    return {"clients": clients, "workers": args.workers, "duration": args.duration, "warmup": args.warmup,
            "keys": args.keys, "value_size": args.value_size, "read_ratio": args.read_ratio,
            "pipeline": pipeline, "rate": rate}


# -- commands ----------------------------------------------------------------

def cmd_run(args):
    server_cpus, client_cpus = cpu_layout()
    server = Server(args.server, server_cpus)
    try:
        preload(server.port, args.keys, args.value_size)
        r = run(config(args, args.clients, args.rate, args.pipeline), server, client_cpus)
    finally:
        server.stop()
    print(describe(r))
    if args.save:
        save(args.save, {"meta": meta(args), "runs": [r]})


def cmd_baseline(args):
    server_cpus, client_cpus = cpu_layout()
    print("server on CPU %s, clients on CPUs %s" % (sorted(server_cpus), client_cpus))
    server = Server(args.server, server_cpus)
    runs = []
    try:
        preload(server.port, args.keys, args.value_size)
        for clients in args.client_counts:
            attempts = [run(config(args, clients), server, client_cpus) for _ in range(args.repeats)]
            # The median run by throughput, so one noisy run can't skew it.
            attempts.sort(key=lambda r: r["ops_per_sec"])
            best = attempts[len(attempts) // 2]
            best["repeats_ops_per_sec"] = [r["ops_per_sec"] for r in attempts]
            print(describe(best))
            runs.append(best)
    finally:
        server.stop()
    save("baseline", {"meta": meta(args), "runs": runs})


def cmd_crosscheck(args):
    if not shutil.which("redis-benchmark"):
        sys.exit("redis-benchmark not installed (sudo apt-get install redis-tools)")
    server_cpus, client_cpus = cpu_layout()
    server = Server(args.server, server_cpus)
    runs = []
    try:
        preload(server.port, args.keys, args.value_size)
        for clients in args.client_counts:
            cmd = ["taskset", "-c", ",".join(map(str, client_cpus)), "redis-benchmark", "-p", str(server.port),
                   "-c", str(clients), "-n", str(args.requests), "-t", "get,set", "-d", str(args.value_size),
                   "-r", str(args.keys), "-P", "1", "--csv"]
            out = subprocess.run(cmd, capture_output=True, text=True, check=True).stdout
            for line in out.strip().splitlines()[1:]:
                f = [x.strip('"') for x in line.split(",")]
                run_ = {"clients": clients, "test": f[0], "ops_per_sec": round(float(f[1])),
                        "latency_ms": dict(zip(["avg", "min", "p50", "p95", "p99", "max"], map(float, f[2:8])))}
                print("%4d clients  %-4s %9s ops/s   p50 %.3fms  p99 %.3fms" % (
                    clients, f[0], "{:,}".format(run_["ops_per_sec"]), run_["latency_ms"]["p50"],
                    run_["latency_ms"]["p99"]))
                runs.append(run_)
    finally:
        server.stop()
    save("crosscheck", {"meta": meta(args), "tool": "redis-benchmark", "runs": runs})


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    for name in ("run", "baseline", "crosscheck"):
        s = sub.add_parser(name)
        s.add_argument("--server", choices=["kv", "redis"], default="kv")
        s.add_argument("--keys", type=int, default=100000)
        s.add_argument("--value-size", type=int, default=64)
        s.add_argument("--read-ratio", type=float, default=0.8)
        s.add_argument("--duration", type=float, default=10, help="measured seconds per run")
        s.add_argument("--warmup", type=float, default=2, help="unmeasured seconds before each run")
        s.add_argument("--workers", type=int, default=0, help="client processes (0: one per client, up to the CPUs)")
        if name == "run":
            s.add_argument("--clients", type=int, default=16)
            s.add_argument("--pipeline", type=int, default=1, help="closed loop: requests per batch")
            s.add_argument("--rate", type=int, default=0, help="open loop at this many ops/s in total")
            s.add_argument("--save", metavar="NAME", help="write the result to bench/results/NAME-*.json")
        else:
            s.add_argument("--client-counts", type=int, nargs="+", default=[1, 4, 16, 64])
        if name == "baseline":
            s.add_argument("--repeats", type=int, default=3)
        if name == "crosscheck":
            s.add_argument("--requests", type=int, default=1000000)
    args = p.parse_args()
    {"run": cmd_run, "baseline": cmd_baseline, "crosscheck": cmd_crosscheck}[args.cmd](args)


if __name__ == "__main__":
    main()
