#!/usr/bin/env python3
"""Live failover demo, meant to be recorded for the README (asciinema / GIF).

Starts a 3-master + 3-replica cluster, runs a writer through the cluster
client, SIGKILLs one master mid-traffic, and narrates what happens:
suspicion (PFAIL), agreement (FAIL), the replica's election, and writes
resuming. Then it verifies every acknowledged write, restarts the old master
and shows it rejoin as a replica of the node that replaced it.

Usage (Linux/WSL, after building):
  python3 tools/demo_failover.py
  python3 tools/demo_failover.py --server build-release/src/kv_server --fast
"""

import argparse
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.join(HERE, "..")
sys.path.insert(0, HERE)
import kv_cluster  # noqa: E402
from kv_cluster import ClusterClient, Conn, ReplyError, cluster_nodes, wait_until  # noqa: E402
from kvclient import UncertainWriteError  # noqa: E402

BOLD, DIM, RED, GREEN, YELLOW, CYAN, RESET = "\033[1m", "\033[2m", "\033[31m", "\033[32m", "\033[33m", "\033[36m", "\033[0m"
T0 = time.monotonic()
KILLED_AT = None


def say(msg, color=""):
    t = time.monotonic() - (KILLED_AT or T0)
    stamp = ("t=%+6.2fs" % t) if KILLED_AT else ("%7.2fs " % t)
    print("%s[%s]%s %s%s%s" % (DIM, stamp, RESET, color, msg, RESET), flush=True)


def banner(msg):
    print("\n%s%s== %s ==%s" % (BOLD, CYAN, msg, RESET), flush=True)


class Node:
    def __init__(self, server, port, node_timeout, root):
        self.server, self.port, self.node_timeout = server, port, node_timeout
        self.dir = os.path.join(root, str(port))
        os.makedirs(self.dir, exist_ok=True)
        self.proc = None
        self.id = None
        self.start()

    def start(self):
        with socket.socket() as probe:
            if probe.connect_ex(("127.0.0.1", self.port)) == 0:
                sys.exit("port %d is already in use; pass --base-port" % self.port)
        log = open(os.path.join(self.dir, "server.log"), "ab")
        self.proc = subprocess.Popen(
            [self.server, "--port", str(self.port), "--dir", self.dir, "--save", "",
             "--cluster-enabled", "yes", "--cluster-node-timeout", str(self.node_timeout)],
            stdout=log, stderr=log)
        wait_until(lambda: socket.create_connection(("127.0.0.1", self.port), 0.2).close() or True,
                   5, "port %d accepting" % self.port)

    @property
    def addr(self):
        return ("127.0.0.1", self.port)

    def nodes(self):
        c = Conn(*self.addr, timeout=1)
        try:
            return cluster_nodes(c)
        finally:
            c.close()

    def me(self):
        return next(n for n in self.nodes() if "myself" in n["flags"])

    def kill(self):
        if self.proc and self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait()


def slot_ranges(slots):
    out, s = [], sorted(slots)
    i = 0
    while i < len(s):
        j = i
        while j + 1 < len(s) and s[j + 1] == s[j] + 1:
            j += 1
        out.append("%d-%d" % (s[i], s[j]) if j > i else str(s[i]))
        i = j + 1
    return ",".join(out) or "-"


def show_topology(view_from, label_of):
    for n in sorted(view_from.nodes(), key=lambda n: n["port"]):
        name = label_of.get(n["id"], n["id"][:8])
        flags = [f for f in n["flags"] if f != "myself"]
        state = RED + "FAIL" + RESET if "fail" in flags else (YELLOW + "PFAIL" + RESET if "fail?" in flags else GREEN + "up" + RESET)
        if "master" in flags:
            role = "master   slots %-12s epoch %d" % (slot_ranges(n["slots"]), n["epoch"])
        else:
            role = "replica  of %s" % label_of.get(n["master"], (n["master"] or "?")[:8])
        print("   :%d  %-3s %-6s %s" % (n["port"], name, state, role), flush=True)


class Writer(threading.Thread):
    """One client writing unique keys (demo:<i> = i) as fast as it can."""

    def __init__(self, seed):
        super().__init__(daemon=True)
        self.client = ClusterClient(seed, retry_timeout=60, timeout=2)
        self.acked, self.uncertain = {}, set()
        self.stop = threading.Event()
        self.lock = threading.Lock()
        self.longest_stall, self.stall_started, self.last_ok = 0.0, None, time.monotonic()

    def run(self):
        i = 0
        while not self.stop.is_set():
            key = "demo:%d" % i
            began = time.monotonic()
            try:
                self.client.call("SET", key, str(i))
                with self.lock:
                    self.acked[key] = str(i)
            except UncertainWriteError:
                with self.lock:
                    self.uncertain.add(key)  # sent, reply lost: may or may not have applied
            except (ConnectionError, OSError, ReplyError):
                pass
            took = time.monotonic() - began
            if took > self.longest_stall:
                self.longest_stall, self.stall_started = took, began
            i += 1

    def counts(self):
        with self.lock:
            return len(self.acked), len(self.uncertain)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--server", default=os.path.join(REPO, "build", "src", "kv_server"))
    ap.add_argument("--base-port", type=int, default=7000)
    ap.add_argument("--node-timeout", type=int, default=2000, help="ms (Redis default is 15000)")
    ap.add_argument("--fast", action="store_true", help="no pauses for the viewer")
    ap.add_argument("--keep", action="store_true", help="keep the data directories")
    args = ap.parse_args()
    if not os.path.exists(args.server):
        sys.exit("server binary not found: %s (build first, or pass --server)" % args.server)
    pause = (lambda s: None) if args.fast else time.sleep

    root = tempfile.mkdtemp(prefix="kv_demo_")
    nodes = []
    try:
        banner("Starting 6 nodes: 3 masters + 3 replicas (node timeout %dms)" % args.node_timeout)
        nodes = [Node(args.server, args.base_port + i, args.node_timeout, root) for i in range(6)]
        kv_cluster.create([n.addr for n in nodes], replicas=1)
        for n in nodes:
            n.id = n.me()["id"]
        masters, replicas = nodes[:3], nodes[3:]
        label_of = {}
        for i, m in enumerate(masters):
            label_of[m.id] = "M%d" % (i + 1)
            r = next(r for r in replicas if r.me()["master"] == m.id)
            label_of[r.id] = "R%d" % (i + 1)
        say("cluster formed: 16384 slots split across 3 masters, each with a replica", GREEN)
        show_topology(masters[1], label_of)
        pause(2)

        banner("Writer running through the cluster-aware client")
        writer = Writer(masters[1].addr)
        writer.start()
        for _ in range(3):
            time.sleep(1)
            say("writes acknowledged: %d" % writer.counts()[0])

        victim = masters[0]
        vid, vlabel = victim.id, label_of[victim.id]
        heir = next(r for r in replicas if r.me()["master"] == vid)
        witness = masters[1]
        victim_slots = sorted(victim.me()["slots"])
        before_kill = writer.counts()[0]

        banner("kill -9 %s (port %d, slots %s)" % (vlabel, victim.port, slot_ranges(victim_slots)))
        pause(1)
        global KILLED_AT
        KILLED_AT = time.monotonic()
        victim.kill()
        say("%s is dead. No goodbye, no clean shutdown." % vlabel, RED)

        # Narrate the failover as the witness sees it.
        seen, deadline = set(), time.monotonic() + 60
        while time.monotonic() < deadline:
            try:
                view = {n["id"]: n for n in witness.nodes()}
            except (OSError, ReplyError):
                time.sleep(0.05)
                continue
            v, h = view.get(vid), view.get(heir.id)
            if v and "fail?" in v["flags"] and "pfail" not in seen:
                seen.add("pfail")
                say("M2 suspects %s: no PONG within %dms -> PFAIL (a local opinion only)" % (vlabel, args.node_timeout), YELLOW)
            if v and "fail" in v["flags"] and "fail" not in seen:
                seen.add("fail")
                say("a majority of masters agree -> %s marked FAIL, broadcast to every node" % vlabel, RED)
            if h and "master" in h["flags"] and h["slots"] and "promoted" not in seen:
                seen.add("promoted")
                label_of[heir.id] = label_of[heir.id] + "*"
                say("%s won the election (epoch %d) and took slots %s" % (label_of[heir.id], h["epoch"], slot_ranges(h["slots"])), GREEN)
                break
            time.sleep(0.02)
        else:
            sys.exit("failover did not complete within 60s; logs in %s" % root)
        failover_time = time.monotonic() - KILLED_AT

        wait_until(lambda: writer.counts()[0] > before_kill + 500, 30, "writes resuming")
        say("writes flowing again: %d acknowledged so far" % writer.counts()[0], GREEN)
        time.sleep(2)
        writer.stop.set()
        writer.join()

        banner("Topology after failover (as M2 sees it)")
        show_topology(witness, label_of)

        banner("Checking every acknowledged write")
        checker = ClusterClient(witness.addr)
        lost = [k for k, v in writer.acked.items() if checker.call("GET", k) != v.encode()]
        checker.close()
        acked, uncertain = writer.counts()
        say("%d writes acknowledged, %d lost, %d uncertain (reply lost when %s died; reported to the caller, never retried)"
            % (acked, len(lost), uncertain, vlabel), GREEN if not lost else RED)
        if lost:
            say("lost writes are possible: replication is asynchronous, so writes %s acknowledged but had "
                "not yet streamed to its replica die with it" % vlabel, YELLOW)
        say("failover completed %.2fs after the kill; the writer's longest stall was %.2fs"
            % (failover_time, writer.longest_stall), CYAN)
        pause(2)

        banner("Restarting %s: it still thinks it owns its slots" % vlabel)
        victim.start()
        wait_until(lambda: "slave" in victim.me()["flags"] and victim.me()["master"] == heir.id, 30,
                   "the old master becoming a replica")
        say("%s saw %s's higher epoch, gave up its slots, and is now its replica" % (vlabel, label_of[heir.id]), GREEN)
        def witness_agrees():
            v = {n["id"]: n for n in witness.nodes()}[vid]
            return "fail" not in v["flags"] and "slave" in v["flags"] and v["master"] == heir.id
        wait_until(witness_agrees, 30, "gossip reaching M2")
        show_topology(witness, label_of)
        print()
    finally:
        for n in nodes:
            n.kill()
        if args.keep:
            print("data and logs kept in", root)
        else:
            shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
