#!/usr/bin/env python3
"""Cluster admin tool for kv_server (the equivalent of `redis-cli --cluster`).

  kv_cluster.py create HOST:PORT ... [--replicas N]
      Splits the 16384 slots evenly across the masters, joins every node
      into one cluster, and makes N replicas per master of the rest.
  kv_cluster.py reshard --from HOST:PORT --to HOST:PORT --slots N
      Moves N slots, with their keys, from one master to another while
      the cluster keeps serving.
  kv_cluster.py check HOST:PORT
      Verifies that every node agrees on the slot map.

Also importable: the integration tests drive clusters through it.
"""

import argparse
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "python-client"))
from kvclient.cluster import SLOTS, ClusterClient, crc16, key_slot, parse_addr  # noqa: E402,F401
from kvclient.resp import Conn, ReplyError  # noqa: E402,F401


def cluster_nodes(conn):
    """CLUSTER NODES as a list of dicts."""
    nodes = []
    for line in conn.call("CLUSTER", "NODES").decode().splitlines():
        f = line.split()
        addr, cport = f[1].split("@")
        host, port = addr.rsplit(":", 1)
        slots, migrating, importing = set(), {}, {}
        for tok in f[8:]:
            if tok.startswith("["):
                inner = tok[1:-1]
                if "->-" in inner:
                    s, n = inner.split("->-")
                    migrating[int(s)] = n
                else:
                    s, n = inner.split("-<-")
                    importing[int(s)] = n
            elif "-" in tok:
                a, b = tok.split("-")
                slots.update(range(int(a), int(b) + 1))
            else:
                slots.add(int(tok))
        nodes.append({
            "id": f[0], "host": host, "port": int(port) if port else 0, "cport": int(cport), "flags": f[2].split(","),
            "master": None if f[3] == "-" else f[3], "epoch": int(f[6]), "link": f[7],
            "slots": slots, "migrating": migrating, "importing": importing,
        })
    return nodes


def wait_until(predicate, timeout=30, what="condition"):
    deadline = time.time() + timeout
    while True:
        try:
            if predicate():
                return
        except (OSError, ReplyError):
            pass
        if time.time() > deadline:
            raise TimeoutError("%s not reached within %ss" % (what, timeout))
        time.sleep(0.05)


def slot_map(conn):
    """{slot: node id} as this node sees it."""
    owners = {}
    for n in cluster_nodes(conn):
        for s in n["slots"]:
            owners[s] = n["id"]
    return owners


def converged(addrs):
    """True once every node knows every other node (no handshakes left),
    agrees on the slot map, and reports cluster_state:ok."""
    maps = []
    for host, port in addrs:
        c = Conn(host, port)
        try:
            nodes = cluster_nodes(c)
            if len(nodes) != len(addrs) or any("handshake" in n["flags"] for n in nodes):
                return False
            if b"cluster_state:ok" not in c.call("CLUSTER", "INFO"):
                return False
            maps.append(slot_map(c))
        finally:
            c.close()
    return all(m == maps[0] for m in maps)


def create(addrs, replicas=0, timeout=30):
    """Builds a cluster from empty nodes. Returns {addr: node id}."""
    per_shard = replicas + 1
    if len(addrs) % per_shard != 0:
        raise ValueError("%d nodes can't be split into shards of 1 master + %d replicas" % (len(addrs), replicas))
    masters = addrs[:len(addrs) // per_shard]
    others = addrs[len(masters):]
    conns = {a: Conn(*a) for a in addrs}
    try:
        ids = {a: c.call("CLUSTER", "MYID").decode() for a, c in conns.items()}
        # Distinct config epochs up front, so no slot claim ever ties.
        for i, a in enumerate(masters):
            conns[a].call("CLUSTER", "SET-CONFIG-EPOCH", i + 1)
            start = SLOTS * i // len(masters)
            end = SLOTS * (i + 1) // len(masters) - 1
            conns[a].call("CLUSTER", "ADDSLOTSRANGE", start, end)
        # One MEET each is enough: gossip introduces everyone to everyone.
        first = addrs[0]
        for a in addrs[1:]:
            conns[a].call("CLUSTER", "MEET", first[0], first[1], bus_port(conns[first]))

        def all_know_all():
            return all(len([n for n in cluster_nodes(c) if "handshake" not in n["flags"]]) == len(addrs)
                       for c in conns.values())
        wait_until(all_know_all, timeout, "every node knowing every other node")
        for i, a in enumerate(others):
            conns[a].call("CLUSTER", "REPLICATE", ids[masters[i % len(masters)]])
        wait_until(lambda: converged(addrs), timeout, "cluster convergence")
        return ids
    finally:
        for c in conns.values():
            c.close()


def bus_port(conn):
    return next(n["cport"] for n in cluster_nodes(conn) if "myself" in n["flags"])


def migrate_slot(src, dst, slot, batch=100, timeout_ms=5000):
    """Live-migrates one slot: IMPORTING on the target first (so it accepts
    the ASKs), MIGRATING on the source, move keys in batches, then hand the
    slot over on the target (which bumps its epoch) and the source."""
    src_id = src.call("CLUSTER", "MYID").decode()
    dst_id = dst.call("CLUSTER", "MYID").decode()
    dst.call("CLUSTER", "SETSLOT", slot, "IMPORTING", src_id)
    src.call("CLUSTER", "SETSLOT", slot, "MIGRATING", dst_id)
    moved = 0
    while True:
        keys = src.call("CLUSTER", "GETKEYSINSLOT", slot, batch)
        if not keys:
            break
        # REPLACE: a retry after a timeout may find a key already copied.
        src.call("MIGRATE", dst.host, dst.port, "", 0, timeout_ms, "REPLACE", "KEYS", *keys)
        moved += len(keys)
    dst.call("CLUSTER", "SETSLOT", slot, "NODE", dst_id)
    src.call("CLUSTER", "SETSLOT", slot, "NODE", dst_id)
    return moved


def reshard(src_addr, dst_addr, count=None, slots=None, batch=100):
    """Moves `count` slots (or exactly `slots`) from src to dst."""
    src, dst = Conn(*src_addr), Conn(*dst_addr)
    try:
        src_id = src.call("CLUSTER", "MYID").decode()
        owned = sorted(s for s, n in slot_map(src).items() if n == src_id)
        chosen = slots if slots is not None else owned[:count]
        moved = sum(migrate_slot(src, dst, s, batch) for s in chosen)
        return len(chosen), moved
    finally:
        src.close()
        dst.close()



def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("create")
    c.add_argument("nodes", nargs="+")
    c.add_argument("--replicas", type=int, default=0)
    r = sub.add_parser("reshard")
    r.add_argument("--from", dest="src", required=True)
    r.add_argument("--to", dest="dst", required=True)
    r.add_argument("--slots", type=int, required=True)
    k = sub.add_parser("check")
    k.add_argument("node")
    a = p.parse_args()

    if a.cmd == "create":
        ids = create([parse_addr(n) for n in a.nodes], a.replicas)
        for addr, nid in ids.items():
            print("%s:%d %s" % (addr[0], addr[1], nid))
        print("cluster created")
    elif a.cmd == "reshard":
        n, keys = reshard(parse_addr(a.src), parse_addr(a.dst), count=a.slots)
        print("moved %d slots, %d keys" % (n, keys))
    else:
        seed = Conn(*parse_addr(a.node))
        addrs = [(n["host"], n["port"]) for n in cluster_nodes(seed)]
        seed.close()
        ok = converged(addrs)
        print("OK: all %d nodes agree on the slot map" % len(addrs) if ok else "MISMATCH")
        sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
