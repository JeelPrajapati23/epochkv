#!/usr/bin/env python3
"""The in-cluster half of bench/netem.py: runs in the client container, on
the same Docker network as the nodes, and prints one JSON document.

  netem_client.py watch --nodes IP:PORT,... --seconds S [--stale-target IP:PORT]

For S seconds:
  - a writer sets unique keys through the cluster client, as fast as it
    can, recording which writes were acknowledged;
  - every 100ms, each reachable node is asked for CLUSTER NODES and CLUSTER
    INFO, and every change in what it believes is recorded with a wall-clock
    timestamp: a peer gaining or losing the fail? (PFAIL) or fail flag, a
    peer's role changing, its own cluster_state changing;
  - with --stale-target, a second writer talks to that one node directly,
    not through the cluster client, on a key in its own slots. It plays an
    old client still connected to a master that froze and was replaced:
    every OK it gets after the freeze is a write the cluster will lose.
Then every acknowledged write is read back, and the missing ones counted.

Wall-clock times, so the host script can line them up with when it killed
or paused a container (both run on the same WSL2 kernel, so one clock).
"""

import argparse
import json
import os
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "tools"))
sys.path.insert(0, os.path.join(HERE, "..", "python-client"))
from demo_failover import Writer  # noqa: E402
from kv_cluster import ClusterClient, Conn, ReplyError, cluster_nodes, key_slot  # noqa: E402
from kvclient import UncertainWriteError  # noqa: E402


class TimedWriter(Writer):
    """The failover demo's writer, also recording when each write was
    acknowledged and when each uncertain one was given up on, so a lost
    write can be placed on the failover's timeline."""

    def __init__(self, seed):
        super().__init__(seed)
        self.acked_at, self.uncertain_at = {}, {}

    def run(self):
        i = 0
        while not self.stop.is_set():
            key = "demo:%d" % i
            began = time.monotonic()
            try:
                self.client.call("SET", key, str(i))
                with self.lock:
                    self.acked[key] = str(i)
                    self.acked_at[key] = time.time()
            except UncertainWriteError:
                with self.lock:
                    self.uncertain.add(key)
                    self.uncertain_at[key] = time.time()
            except (ConnectionError, OSError, ReplyError):
                pass
            took = time.monotonic() - began
            if took > self.longest_stall:
                self.longest_stall, self.stall_started = took, began
            i += 1


def parse_addr(s):
    host, _, port = s.rpartition(":")
    return host, int(port)


class StaleWriter(threading.Thread):
    """Writes straight to one node, on a key it owns, ignoring redirects:
    records the time and kind of every reply."""

    def __init__(self, addr, key):
        super().__init__(daemon=True)
        self.addr, self.key = addr, key
        self.replies = []  # (time, "ok" | error prefix | "timeout")
        self.stop = threading.Event()

    def run(self):
        conn, i = None, 0
        while not self.stop.is_set():
            try:
                if conn is None:
                    conn = Conn(*self.addr, timeout=0.5)
                conn.call("SET", self.key, str(i))
                self.replies.append((time.time(), "ok", i))
            except ReplyError as e:
                self.replies.append((time.time(), str(e).split()[0], i))
                time.sleep(0.01)
            except OSError:
                self.replies.append((time.time(), "timeout", i))
                if conn is not None:
                    conn.close()
                conn = None
            i += 1


def key_owned_by(conn, node_id):
    """A key whose slot `node_id` serves."""
    slots = next(n["slots"] for n in cluster_nodes(conn) if n["id"] == node_id)
    i = 0
    while True:
        key = "stale:{%d}" % i
        if key_slot(key) in slots:
            return key
        i += 1


def watch(args):
    addrs = [parse_addr(a) for a in args.nodes.split(",")]
    conns, views, events = {}, {}, []
    down_since = {}  # addr -> when we last failed to reach it

    def poll():
        now = time.time()
        for addr in addrs:
            observer = "%s:%d" % addr
            # Retry an unreachable node only every 2s: each attempt can take
            # the whole connect timeout, which would slow polling for the rest.
            if now - down_since.get(addr, 0) < 2:
                continue
            try:
                if addr not in conns:
                    conns[addr] = Conn(*addr, timeout=0.3)
                c = conns[addr]
                nodes = cluster_nodes(c)
                state = c.call("CLUSTER", "INFO").decode().split("cluster_state:")[1].split()[0]
            except (OSError, ReplyError):
                if addr in conns:
                    conns.pop(addr).close()
                down_since[addr] = time.time()
                continue
            view = {"state": state}
            for n in nodes:
                subject = "%s:%d" % (n["host"], n["port"])
                flags = set(n["flags"])
                view[n["id"]] = (subject, "fail?" in flags, "fail" in flags,
                                 "master" if "master" in flags else "replica")
            old = views.get(observer)
            if old is not None:
                if old["state"] != state:
                    events.append({"t": now, "observer": observer, "event": "state", "value": state})
                for node_id, entry in view.items():
                    if node_id == "state" or node_id not in old:
                        continue
                    subject, pfail, fail, role = entry
                    _, opfail, ofail, orole = old[node_id]
                    for name, was, now_ in (("pfail", opfail, pfail), ("fail", ofail, fail)):
                        if was != now_:
                            events.append({"t": now, "observer": observer, "event": name, "value": now_,
                                           "subject": subject, "subject_id": node_id})
                    if orole != role:
                        events.append({"t": now, "observer": observer, "event": "role", "value": role,
                                       "subject": subject, "subject_id": node_id})
            views[observer] = view

    seed = addrs[0]
    c = Conn(*seed)
    topology = [{"id": n["id"], "addr": "%s:%d" % (n["host"], n["port"]), "master": n["master"],
                 "slots": len(n["slots"])} for n in cluster_nodes(c)]
    stale = None
    if args.stale_target:
        target = parse_addr(args.stale_target)
        t = Conn(*target)
        target_id = t.call("CLUSTER", "MYID").decode()
        t.close()
        stale = StaleWriter(target, key_owned_by(c, target_id))
    c.close()

    writer = TimedWriter(seed)
    writer.start()
    if stale:
        stale.start()
    started = time.time()
    while time.time() < started + args.seconds:
        poll()
        time.sleep(0.1)
    writer.stop.set()
    writer.join(timeout=70)
    if stale:
        stale.stop.set()
        stale.join(timeout=5)

    # Read back every acknowledged write.
    client = ClusterClient(seed, retry_timeout=30, timeout=2)
    lost = []
    with writer.lock:
        acked = dict(writer.acked)
    keys = list(acked)
    for i in range(0, len(keys), 1000):
        pipe = client.pipeline()
        for k in keys[i:i + 1000]:
            pipe.call("GET", k)
        for k, v in zip(keys[i:i + 1000], pipe.execute(raise_on_error=False)):
            if v != acked[k].encode():
                # An error here is a failed read, not a lost write: kept apart.
                lost.append({"key": k, "slot": key_slot(k), "got": repr(v), "read_error": isinstance(v, Exception),
                             "acked_at": writer.acked_at[k]})
    result = {"started": started, "topology": topology, "events": events,
              "writer": {"acked": len(acked), "uncertain": len(writer.uncertain),
                         "lost": len([x for x in lost if not x["read_error"]]),
                         "read_errors": len([x for x in lost if x["read_error"]]), "lost_detail": lost[:20],
                         "longest_stall_s": round(writer.longest_stall, 3),
                         "longest_stall_started": writer.stall_started,
                         "uncertain_at": sorted(writer.uncertain_at.items(), key=lambda kv: kv[1])}}
    if stale:
        final = client.call("GET", stale.key)
        result["stale"] = {"key": stale.key, "replies": stale.replies,
                           "final_value": final.decode() if final is not None else None}
    print(json.dumps(result))


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    w = sub.add_parser("watch")
    w.add_argument("--nodes", required=True)
    w.add_argument("--seconds", type=float, required=True)
    w.add_argument("--stale-target")
    args = p.parse_args()
    watch(args)


if __name__ == "__main__":
    main()
