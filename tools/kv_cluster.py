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
import socket
import sys
import time

SLOTS = 16384


class ReplyError(Exception):
    pass


class Conn:
    """Minimal blocking RESP client. Error replies raise ReplyError."""

    def __init__(self, host, port, timeout=10):
        self.host, self.port = host, int(port)
        self.sock = socket.create_connection((self.host, self.port), timeout=timeout)
        self.buf = b""

    def close(self):
        self.sock.close()

    def _line(self):
        while b"\r\n" not in self.buf:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError("connection closed by %s:%d" % (self.host, self.port))
            self.buf += chunk
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def _reply(self):
        line = self._line()
        kind, rest = line[:1], line[1:]
        if kind == b"+":
            return rest.decode()
        if kind == b"-":
            return ReplyError(rest.decode())
        if kind == b":":
            return int(rest)
        if kind == b"$":
            n = int(rest)
            if n < 0:
                return None
            while len(self.buf) < n + 2:
                chunk = self.sock.recv(65536)
                if not chunk:
                    raise ConnectionError("connection closed mid-reply")
                self.buf += chunk
            data, self.buf = self.buf[:n], self.buf[n + 2:]
            return data
        if kind == b"*":
            n = int(rest)
            return None if n < 0 else [self._reply() for _ in range(n)]
        raise ValueError("bad reply line %r" % line)

    def send(self, *args):
        out = b"*%d\r\n" % len(args)
        for a in args:
            a = a if isinstance(a, bytes) else str(a).encode()
            out += b"$%d\r\n%s\r\n" % (len(a), a)
        self.sock.sendall(out)

    def call_raw(self, *args):
        """Returns error replies as ReplyError objects instead of raising."""
        self.send(*args)
        return self._reply()

    def call(self, *args):
        r = self.call_raw(*args)
        if isinstance(r, ReplyError):
            raise r
        return r


def parse_addr(s):
    host, port = s.rsplit(":", 1)
    return host, int(port)


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


class ClusterClient:
    """Routes each command to the node owning its key, following MOVED
    (refresh the slot map) and ASK (retry once at the target with ASKING)."""

    def __init__(self, seed, max_redirects=16):
        self.seed = seed
        self.max_redirects = max_redirects
        self.conns = {}
        self.slots = {}
        self.redirects = {"MOVED": 0, "ASK": 0, "TRYAGAIN": 0}
        self.refresh()

    def conn(self, addr):
        if addr not in self.conns:
            self.conns[addr] = Conn(*addr)
        return self.conns[addr]

    def refresh(self):
        for start, end, master, *_ in self.conn(self.seed).call("CLUSTER", "SLOTS"):
            addr = (master[0].decode(), master[1])
            for s in range(start, end + 1):
                self.slots[s] = addr

    def keyslot(self, key):
        return self.conn(self.seed).call("CLUSTER", "KEYSLOT", key)

    def call(self, *args, key=None):
        key = key if key is not None else args[1]
        addr = self.slots.get(self.keyslot(key), self.seed)
        asking = False
        for _ in range(self.max_redirects):
            c = self.conn(addr)
            if asking:
                c.call("ASKING")
            r = c.call_raw(*args)
            if not isinstance(r, ReplyError):
                return r
            kind, *rest = str(r).split()
            if kind in ("MOVED", "ASK"):
                self.redirects[kind] += 1
                slot, target = int(rest[0]), parse_addr(rest[1])
                addr, asking = target, kind == "ASK"
                if kind == "MOVED":
                    self.slots[slot] = target
            elif kind == "TRYAGAIN":
                self.redirects[kind] += 1
                asking = False
                time.sleep(0.01)
            else:
                raise r
        raise ReplyError("too many redirects for %r" % (args,))

    def close(self):
        for c in self.conns.values():
            c.close()


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
