#!/usr/bin/env python3
"""Memory per key and large-dataset behavior, for kv_server and Redis.

  memory.py perkey [--keys N]
      Loads N keys into a fresh server for several key/value shapes and
      reports how much the server's RSS grew per key, against the raw
      key + value bytes. Also prints our store's own per-key estimate (the
      number maxmemory is enforced against), to show how far it is from
      real memory.
  memory.py scale [--server kv|redis] [--keys N] [--no-replica] [--overwrite-order rand|seq]
      Loads N keys while a probe connection PINGs every millisecond, then
      runs BGSAVE (under overwrite load, to show copy-on-write), a restart
      from the snapshot, and a full resync to a fresh replica. The probe's
      worst latencies show every moment the server stopped answering:
      hash table resizes, fork(), and so on.
      The BGSAVE overwrites go in a scattered order by default. In insertion
      order (seq) they walk memory in allocation order, so one copied page
      covers many overwrites and copy-on-write looks ~10x cheaper than under
      a realistic access pattern; results before this option existed used seq.

Server data goes under ~/.cache/kvbench, not /tmp: on WSL /tmp is a tmpfs,
so snapshot files there would eat the RAM being measured.
Linux only (/proc). Runs the Release build.
"""

import argparse
import json
import multiprocessing as mp
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import bench  # noqa: E402
from bench import KV_BIN, cpu_layout, free_port, git_commit, machine_info, save  # noqa: E402
from kvclient.resp import Conn  # noqa: E402

DATA_ROOT = os.path.expanduser("~/.cache/kvbench")
BATCH = 1000


# -- processes -----------------------------------------------------------------

def rss_bytes(pid):
    with open("/proc/%d/status" % pid) as f:
        for line in f:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
    return 0


def private_dirty_bytes(pid):
    """Pages this process has written since it forked: for a snapshot child,
    exactly the copy-on-write copies."""
    try:
        with open("/proc/%d/smaps_rollup" % pid) as f:
            return sum(int(line.split()[1]) * 1024 for line in f if line.startswith("Private_Dirty:"))
    except OSError:
        return 0


def children(pid):
    try:
        with open("/proc/%d/task/%d/children" % (pid, pid)) as f:
            return [int(p) for p in f.read().split()]
    except OSError:
        return []


class Server:
    def __init__(self, kind, cpus, dir=None, extra=()):
        self.kind = kind
        self.port = free_port()
        os.makedirs(DATA_ROOT, exist_ok=True)
        self.dir = dir or tempfile.mkdtemp(prefix="%s-" % kind, dir=DATA_ROOT)
        if kind == "kv":
            cmd = [KV_BIN, "--port", str(self.port), "--dir", self.dir, "--save", ""]
        else:
            cmd = ["redis-server", "--port", str(self.port), "--dir", self.dir, "--save", "",
                   "--appendonly", "no", "--protected-mode", "no", "--daemonize", "no"]
        cmd += list(extra)
        self.started = time.monotonic()
        self.proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        os.sched_setaffinity(self.proc.pid, cpus)
        self.ready_after = self._wait_ready()

    def _wait_ready(self, timeout=600):
        """Seconds until the server answers PING. kv_server loads its data
        before it listens; Redis listens first and replies -LOADING."""
        deadline = self.started + timeout
        while True:
            try:
                c = Conn("127.0.0.1", self.port, timeout=5)
                try:
                    c.call("PING")
                    return time.monotonic() - self.started
                finally:
                    c.close()
            except Exception:
                if time.monotonic() > deadline or self.proc.poll() is not None:
                    raise RuntimeError("%s didn't start" % self.kind)
                time.sleep(0.01)

    def conn(self):
        return Conn("127.0.0.1", self.port, timeout=600)

    def call(self, *args):
        c = self.conn()
        try:
            return c.call(*args)
        finally:
            c.close()

    def rss(self):
        return rss_bytes(self.proc.pid)

    def stop(self, keep_dir=False):
        self.proc.send_signal(signal.SIGKILL)  # no save-on-exit: the next step decides what's on disk
        self.proc.wait(30)
        if not keep_dir:
            shutil.rmtree(self.dir, ignore_errors=True)


def settle(server):
    """RSS after the allocator and any background work have calmed down."""
    time.sleep(0.5)
    return server.rss()


# -- loading -------------------------------------------------------------------

def key_name(i, key_len):
    return ("key:%0*d" % (key_len - 4, i)).encode()


def encode_set(key, value, ttl):
    parts = [b"SET", key, value] + ([b"EX", b"1000000"] if ttl else [])
    out = [b"*%d\r\n" % len(parts)]
    for p in parts:
        out.append(b"$%d\r\n%s\r\n" % (len(p), p))
    return b"".join(out)


# Odd and prime, so j -> j * SCATTER mod N is a bijection on 0..N-1 for any N
# it doesn't divide: every key is still hit once per lap, in a scattered order.
SCATTER = 2654435761


def load(server, keys, key_len, value_size, ttl=False, wrap=None, progress=None, scatter=False):
    """Pipelined SETs in batches of BATCH over one connection. Returns the
    elapsed seconds; progress(t, keys_done) is called after each batch, and
    returning True stops early. wrap=N overwrites keys 0..N-1 cyclically,
    in a scattered order if scatter is set."""
    if scatter:
        assert wrap and wrap % SCATTER != 0

    def index(i):
        if not wrap:
            return i
        return (i % wrap) * SCATTER % wrap if scatter else i % wrap

    value = b"x" * value_size
    sock = socket.create_connection(("127.0.0.1", server.port))
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    ok = b"+OK\r\n"
    t0 = time.monotonic()
    for lo in range(0, keys, BATCH):
        hi = min(lo + BATCH, keys)
        sock.sendall(b"".join(encode_set(key_name(index(i), key_len), value, ttl)
                              for i in range(lo, hi)))
        want = len(ok) * (hi - lo)
        buf = bytearray()
        while len(buf) < want:
            chunk = sock.recv(want - len(buf))
            if not chunk:
                raise RuntimeError("server closed the connection")
            buf += chunk
        if buf != ok * (hi - lo):
            raise RuntimeError("unexpected reply: %r" % bytes(buf[:80]))
        if progress and progress(time.monotonic(), hi):
            break
    sock.close()
    return time.monotonic() - t0


# -- probe ---------------------------------------------------------------------

def probe_main(port, cpus, stop, out):
    """PINGs every millisecond. A PING that should have gone out while the
    server was stuck goes out late, so latency counts from the *scheduled*
    time: a 500ms stall shows up as ~500 samples, the worst being ~500ms."""
    os.sched_setaffinity(0, cpus)
    sock = socket.create_connection(("127.0.0.1", port))
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    samples = []
    period = 0.001
    next_at = time.monotonic()
    while not stop.is_set():
        now = time.monotonic()
        if now < next_at:
            time.sleep(next_at - now)
        sock.sendall(b"*1\r\n$4\r\nPING\r\n")
        buf = b""
        while len(buf) < 7:
            buf += sock.recv(7 - len(buf))
        done = time.monotonic()
        samples.append((next_at, done - next_at))
        next_at += period
        if next_at < done - 1:  # don't pile up an unbounded backlog after a long stall
            next_at = done
    sock.close()
    out.send(samples)
    out.close()


class Probe:
    def __init__(self, port, cpus):
        self.stop_event = mp.Event()
        self.rx, tx = mp.Pipe(duplex=False)
        self.proc = mp.Process(target=probe_main, args=(port, cpus, self.stop_event, tx), daemon=True)
        self.proc.start()

    def finish(self):
        self.stop_event.set()
        samples = self.rx.recv()
        self.proc.join(10)
        return samples


def stalls(samples, t_from=None, t_to=None, threshold=0.010):
    """Merges consecutive late PINGs into stall episodes: (start, worst)."""
    out = []
    for t, lat in samples:
        if (t_from is not None and t < t_from) or (t_to is not None and t > t_to):
            continue
        if lat < threshold:
            continue
        if out and t <= out[-1][0] + out[-1][1] + 0.002:
            out[-1] = (out[-1][0], max(out[-1][1], lat))
        else:
            out.append((t, lat))
    return out


def worst(samples, t_from, t_to):
    lats = [lat for t, lat in samples if t_from <= t <= t_to]
    return max(lats) if lats else 0.0


def pct(samples, q):
    lats = sorted(lat for _, lat in samples)
    return lats[min(len(lats) - 1, int(q * len(lats)))] if lats else 0.0


# -- perkey --------------------------------------------------------------------

SHAPES = [
    # (label, key length, value size, ttl)
    ("12B key, 8B value", 12, 8, False),
    ("12B key, 64B value", 12, 64, False),
    ("12B key, 256B value", 12, 256, False),
    ("32B key, 64B value", 32, 64, False),
    ("12B key, 64B value, TTL", 12, 64, True),
]


def kv_estimate(key_len, value_size, ttl):
    """src/store.cpp's entry_cost + expire_cost: what maxmemory counts."""
    return 128 + 2 * key_len + value_size + ((64 + key_len) if ttl else 0)


def cmd_perkey(args):
    server_cpus, client_cpus = cpu_layout(1)
    rows = []
    for label, key_len, value_size, ttl in SHAPES:
        row = {"shape": label, "key_len": key_len, "value_size": value_size, "ttl": ttl,
               "raw": key_len + value_size}
        for kind in ("kv", "redis"):
            s = Server(kind, set(server_cpus))
            try:
                before = settle(s)
                load(s, args.keys, key_len, value_size, ttl)
                after = settle(s)
                assert int(s.call("DBSIZE")) == args.keys
                row[kind] = (after - before) / args.keys
                if kind == "redis":
                    info = s.call("INFO", "memory").decode()
                    used = int(next(l.split(":")[1] for l in info.split("\r\n") if l.startswith("used_memory:")))
                    row["redis_used_memory"] = used / args.keys
            finally:
                s.stop()
        row["kv_estimate"] = kv_estimate(key_len, value_size, ttl)
        rows.append(row)
        print("%-26s raw %4d B | kv %6.0f B/key (estimate %4d) | redis %6.0f B/key | kv/redis %.2fx"
              % (label, row["raw"], row["kv"], row["kv_estimate"], row["redis"], row["kv"] / row["redis"]))
    doc = {"meta": {"server": "both", "commit": git_commit(), "date": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "machine": machine_info(), "keys": args.keys}, "rows": rows}
    save("memory-perkey", doc)


# -- scale ---------------------------------------------------------------------

def mb(n):
    return "%.0f MB" % (n / 1e6)


def ms(s):
    return "%.1f ms" % (s * 1e3)


def overwrite_until(server, stop_when, key_len, value_size, total_keys, scatter=False):
    """Overwrites existing keys, wrapping around, until stop_when() is true:
    in insertion order, or a scattered order if scatter is set. Returns how
    many were written."""
    written = [0]

    def progress(_t, n):
        written[0] = n
        return stop_when()

    if not stop_when():
        load(server, 1 << 62, key_len, value_size, wrap=total_keys, progress=progress, scatter=scatter)
    return written[0]


def cmd_scale(args):
    server_cpus, client_cpus = cpu_layout(1)
    probe_cpus = {client_cpus[0]}
    key_len, value_size = 12, args.value_size
    result = {"keys": args.keys, "key_len": key_len, "value_size": value_size,
              "overwrite_order": args.overwrite_order}

    s = Server(args.server, set(server_cpus))
    try:
        base_rss = settle(s)
        probe = Probe(s.port, probe_cpus)
        time.sleep(0.2)

        # 1. Load, recording when each batch finished so stalls can be
        #    matched to the key count at that moment.
        marks = []
        elapsed = load(s, args.keys, key_len, value_size, progress=lambda t, n: marks.append((t, n)))
        load_end = time.monotonic()
        loaded_rss = settle(s)
        result["load"] = {"seconds": elapsed, "ops_per_sec": args.keys / elapsed,
                          "rss": loaded_rss, "bytes_per_key": (loaded_rss - base_rss) / args.keys}
        print("load     %d keys in %.1fs (%.0f SET/s), RSS %s = %.0f B/key"
              % (args.keys, elapsed, args.keys / elapsed, mb(loaded_rss), result["load"]["bytes_per_key"]))

        # 2. BGSAVE while overwriting keys: the child's Private_Dirty is the
        #    memory copy-on-write cost for the parent's writes.
        snap = os.path.join(s.dir, "dump.snap" if args.server == "kv" else "dump.rdb")
        t_bgsave = time.monotonic()
        s.call("BGSAVE")
        peak_cow = [0]
        peak_total = [0]

        def done():
            kids = children(s.proc.pid)
            for k in kids:
                peak_cow[0] = max(peak_cow[0], private_dirty_bytes(k))
                try:
                    peak_total[0] = max(peak_total[0], s.rss() + private_dirty_bytes(k))
                except OSError:
                    pass
            return not kids and time.monotonic() - t_bgsave > 0.05 and os.path.exists(snap)

        written = overwrite_until(s, done, key_len, value_size, args.keys,
                                  scatter=args.overwrite_order == "rand")
        bgsave_s = time.monotonic() - t_bgsave
        result["bgsave"] = {"seconds": bgsave_s, "file_bytes": os.path.getsize(snap),
                            "overwrites_during": written, "peak_cow_bytes": peak_cow[0],
                            "peak_parent_plus_cow": peak_total[0]}
        print("bgsave   %.1fs, file %s, %d %s overwrites during it -> child copied %s (COW), peak ~%s"
              % (bgsave_s, mb(os.path.getsize(snap)), written, args.overwrite_order, mb(peak_cow[0]),
                 mb(peak_total[0])))
        bgsave_end = time.monotonic()

        # 3. Full resync to an empty replica.
        if not args.no_replica:
            t_sync = time.monotonic()
            r = Server(args.server, {client_cpus[1]}, extra=["--replicaof", "127.0.0.1", str(s.port)])
            try:
                while True:
                    info = r.call("INFO", "replication").decode()
                    if "master_link_status:up" in info and int(r.call("DBSIZE")) >= args.keys:
                        break
                    time.sleep(0.05)
                sync_s = time.monotonic() - t_sync
                result["full_resync"] = {"seconds": sync_s, "replica_rss": settle(r)}
                print("resync   empty replica caught up in %.1fs, replica RSS %s"
                      % (sync_s, mb(result["full_resync"]["replica_rss"])))
            finally:
                r.stop()
        sync_end = time.monotonic()

        samples = probe.finish()

        # Probe results per phase.
        def phase(name, t0, t1):
            eps = stalls(samples, t0, t1)
            top = sorted(eps, key=lambda e: -e[1])[:args.top]
            result.setdefault("stalls", {})[name] = {
                "worst": worst(samples, t0, t1), "episodes_over_10ms": len(eps),
                "top": [{"at_keys": keys_at(marks, t), "worst": lat} for t, lat in top]}
            return eps, top

        def keys_at(marks, t):
            n = 0
            for mt, mn in marks:
                if mt > t:
                    break
                n = mn
            return n

        load_samples = [x for x in samples if x[0] <= load_end]
        print("\nprobe    PING p50 %s, p99 %s, p99.9 %s during load"
              % (ms(pct(load_samples, .5)), ms(pct(load_samples, .99)), ms(pct(load_samples, .999))))
        for name, t0, t1 in (("load", 0, load_end), ("bgsave", t_bgsave, bgsave_end),
                             ("resync", bgsave_end, sync_end)):
            eps, top = phase(name, t0, t1)
            print("  %-7s worst %s, %d stalls > 10 ms" % (name, ms(result["stalls"][name]["worst"]), len(eps)))
            if name == "load":
                for e in sorted(result["stalls"][name]["top"], key=lambda e: e["at_keys"]):
                    print("           %8s at ~%d keys" % (ms(e["worst"]), e["at_keys"]))

        # 4. Restart from the snapshot.
        s.stop(keep_dir=True)
        r = Server(args.server, set(server_cpus), dir=s.dir)
        try:
            # Redis answers PING while still loading; wait for the data too.
            while int(r.call("DBSIZE")) < args.keys:
                time.sleep(0.01)
            restart_s = time.monotonic() - r.started
            result["restart"] = {"seconds": restart_s, "rss": settle(r)}
            print("\nrestart  %d keys loaded from the snapshot in %.1fs, RSS %s (was %s before)"
                  % (args.keys, restart_s, mb(result["restart"]["rss"]), mb(loaded_rss)))
        finally:
            r.stop()
    finally:
        if s.proc.poll() is None:
            s.stop()

    doc = {"meta": {"server": args.server, "commit": git_commit(), "date": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "machine": machine_info()}, "result": result}
    save("memory-scale-%dk" % (args.keys // 1000), doc)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("perkey")
    s.add_argument("--keys", type=int, default=1_000_000)
    s = sub.add_parser("scale")
    s.add_argument("--server", choices=["kv", "redis"], default="kv")
    s.add_argument("--keys", type=int, default=5_000_000)
    s.add_argument("--value-size", type=int, default=64)
    s.add_argument("--no-replica", action="store_true", help="skip the full-resync step (halves peak RAM)")
    s.add_argument("--top", type=int, default=8, help="how many of the worst load stalls to list")
    s.add_argument("--overwrite-order", choices=["rand", "seq"], default="rand",
                   help="order of the overwrites during BGSAVE (seq = insertion order, best case for COW)")
    args = p.parse_args()
    {"perkey": cmd_perkey, "scale": cmd_scale}[args.cmd](args)


if __name__ == "__main__":
    main()
