"""End-to-end sharding tests: several kv_server processes in cluster mode.

Usage: python3 tests/integration/test_cluster.py [path/to/kv_server]
"""

import os
import random
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "..", "..", "tools"))
import test_server  # noqa: E402
from test_server import free_port  # noqa: E402
import kv_cluster  # noqa: E402
from kv_cluster import ClusterClient, Conn, ReplyError, cluster_nodes, converged, wait_until  # noqa: E402


class Node:
    """One cluster-mode kv_server with its own data directory and ports."""

    def __init__(self, *args, data_dir=None, port=None, cport=None):
        self.dir = data_dir or tempfile.mkdtemp(prefix="kv_store_cluster_")
        self.port = port or free_port()
        self.cport = cport or free_port()
        self.args = args
        self.start()

    def start(self):
        self.proc = subprocess.Popen(
            [test_server.SERVER_BIN, "--port", str(self.port), "--dir", self.dir, "--save", "",
             "--cluster-enabled", "yes", "--cluster-port", str(self.cport),
             "--cluster-node-timeout", "2000", *self.args],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = time.time() + 5
        while True:
            try:
                socket.create_connection(("127.0.0.1", self.port), timeout=0.2).close()
                return
            except OSError:
                if time.time() > deadline:
                    raise
                time.sleep(0.02)

    @property
    def addr(self):
        return ("127.0.0.1", self.port)

    def conn(self):
        return Conn(*self.addr)

    def call(self, *args):
        c = self.conn()
        try:
            return c.call_raw(*args)
        finally:
            c.close()

    def myid(self):
        return self.call("CLUSTER", "MYID").decode()

    def nodes(self):
        c = self.conn()
        try:
            return cluster_nodes(c)
        finally:
            c.close()

    def owner_of(self, slot):
        return next((n["id"] for n in self.nodes() if slot in n["slots"]), None)

    def kill(self):
        if self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait()

    def terminate(self):
        self.proc.send_signal(signal.SIGTERM)
        return self.proc.wait(timeout=10)

    def me(self):
        """This node's own CLUSTER NODES entry."""
        return next(x for x in self.nodes() if "myself" in x["flags"])

    def view_of(self, node_id):
        return next((x for x in self.nodes() if x["id"] == node_id), None)

    def info(self, *section):
        text = self.call("INFO", *section).decode()
        return dict(line.split(":", 1) for line in text.splitlines() if ":" in line)

    def cluster_info(self):
        text = self.call("CLUSTER", "INFO").decode()
        return dict(line.split(":", 1) for line in text.splitlines() if ":" in line)

    def get_readonly(self, key):
        c = self.conn()
        try:
            c.call("READONLY")
            return c.call_raw("GET", key)
        finally:
            c.close()


def key_in_slot_range(node, lo, hi, prefix="k"):
    """Some key whose slot is in [lo, hi]."""
    for i in range(100000):
        key = "%s%d" % (prefix, i)
        if lo <= kv_cluster.key_slot(key) <= hi:
            return key
    raise AssertionError("no key found")


def keys_in_slots(slots, count, prefix="k"):
    """`count` keys whose slots are all in the set `slots`."""
    keys = []
    i = 0
    while len(keys) < count:
        key = "%s%d" % (prefix, i)
        if kv_cluster.key_slot(key) in slots:
            keys.append(key)
        i += 1
    return keys


class ClusterTest(unittest.TestCase):
    def setUp(self):
        self.all_nodes = []

    def tearDown(self):
        for n in self.all_nodes:
            n.kill()
            shutil.rmtree(n.dir, ignore_errors=True)

    def node(self, *args, **kwargs):
        n = Node(*args, **kwargs)
        self.all_nodes.append(n)
        return n

    def cluster(self, masters, replicas=0):
        nodes = [self.node() for _ in range(masters * (replicas + 1))]
        kv_cluster.create([n.addr for n in nodes], replicas=replicas)
        return nodes

    def wait_converged(self, nodes, timeout=20):
        wait_until(lambda: converged([n.addr for n in nodes]), timeout, "convergence")

    def wait_serving(self, nodes, timeout=20):
        """Like wait_converged, but tolerates dead nodes the survivors still
        list: every one of `nodes` reports ok and they agree on the slots."""
        def ok():
            maps = []
            for n in nodes:
                if n.cluster_info()["cluster_state"] != "ok":
                    return False
                maps.append({s: x["id"] for x in n.nodes() for s in x["slots"]})
            return all(m == maps[0] for m in maps)
        wait_until(ok, timeout, "the surviving nodes serving every slot")

    def wait_replicated(self, master, replica, timeout=10):
        def caught_up():
            return (replica.info("replication").get("slave_repl_offset") ==
                    master.info("replication")["master_repl_offset"])
        wait_until(caught_up, timeout, "replica catching up with its master")

    def add_replica(self, master, cluster_nodes):
        """A new node that joins the cluster and replicates `master`."""
        n = self.node()
        n.call("CLUSTER", "MEET", "127.0.0.1", master.port, master.cport)
        n_id = n.myid()
        wait_until(lambda: all(x.view_of(n_id) is not None for x in cluster_nodes) and
                   len(n.nodes()) == len(cluster_nodes) + 1, 15, "the new node joining")
        self.assertEqual(n.call("CLUSTER", "REPLICATE", master.myid()), "OK")
        return n

    # --- tests ------------------------------------------------------------------

    def test_create_discovers_everyone_through_gossip(self):
        # create() MEETs every node with the first one only; the rest of the
        # membership has to spread by gossip.
        a, b, c = self.cluster(3)
        for n in (a, b, c):
            view = n.nodes()
            self.assertEqual(len(view), 3)
            self.assertTrue(all(x["link"] == "connected" for x in view))
            self.assertEqual(sum(len(x["slots"]) for x in view), 16384)
            self.assertIn(b"cluster_state:ok", n.call("CLUSTER", "INFO"))
        # Distinct epochs: every slot claim has a clear winner.
        self.assertEqual(sorted(x["epoch"] for x in a.nodes()), [1, 2, 3])

    def test_redirects_and_cluster_client(self):
        a, b, c = self.cluster(3)
        slot = a.call("CLUSTER", "KEYSLOT", "foo")
        owner = next(n for n in (a, b, c) if n.myid() == a.owner_of(slot))
        wrong = next(n for n in (a, b, c) if n is not owner)
        self.assertEqual(str(wrong.call("SET", "foo", "1")), "MOVED %d 127.0.0.1:%d" % (slot, owner.port))
        self.assertEqual(owner.call("SET", "foo", "1"), "OK")
        self.assertTrue(str(owner.call("DEL", "a", "b")).startswith("CROSSSLOT"))
        self.assertEqual(owner.call("CLUSTER", "KEYSLOT", "{foo}bar"), slot)
        self.assertEqual(owner.call("SET", "{foo}bar", "2"), "OK")
        self.assertEqual(owner.call("DEL", "foo", "{foo}bar"), 2)

        client = ClusterClient(a.addr)
        for i in range(500):
            client.call("SET", "key:%d" % i, "v%d" % i)
        for i in range(500):
            self.assertEqual(client.call("GET", "key:%d" % i), b"v%d" % i)
        sizes = [n.call("DBSIZE") for n in (a, b, c)]
        self.assertEqual(sum(sizes), 500)
        self.assertTrue(all(s > 100 for s in sizes), sizes)  # spread over the shards
        client.close()

    def test_migrate_and_ask_by_hand(self):
        a, b = self.cluster(2)
        key = key_in_slot_range(a, 0, 8191)  # owned by a
        slot = a.call("CLUSTER", "KEYSLOT", key)
        a.call("SET", key, "v", "PX", "100000")
        a.call("SET", "{%s}other" % key, "stays")  # same slot (hash tag), moved later
        a_id, b_id = a.myid(), b.myid()

        self.assertEqual(b.call("CLUSTER", "SETSLOT", slot, "IMPORTING", a_id), "OK")
        self.assertEqual(a.call("CLUSTER", "SETSLOT", slot, "MIGRATING", b_id), "OK")
        self.assertEqual(a.call("MIGRATE", "127.0.0.1", b.port, key, 0, 5000), "OK")
        self.assertEqual(a.call("MIGRATE", "127.0.0.1", b.port, key, 0, 5000), "NOKEY")

        # Gone from the source: ASK. The target serves it only after ASKING.
        self.assertEqual(str(a.call("GET", key)), "ASK %d 127.0.0.1:%d" % (slot, b.port))
        self.assertEqual(str(b.call("GET", key)), "MOVED %d 127.0.0.1:%d" % (slot, a.port))
        c = b.conn()
        self.assertEqual(c.call("ASKING"), "OK")
        self.assertEqual(c.call("GET", key), b"v")
        self.assertEqual(c.call_raw("GET", key).__class__, ReplyError)  # ASKING was one-shot
        c.call("ASKING")
        self.assertTrue(90000 < c.call("PTTL", key) <= 100000)  # the TTL travelled too
        c.close()

        remaining = a.call("CLUSTER", "GETKEYSINSLOT", slot, 100)
        if remaining:
            a.call("MIGRATE", "127.0.0.1", b.port, "", 0, 5000, "KEYS", *remaining)
        self.assertEqual(b.call("CLUSTER", "SETSLOT", slot, "NODE", b_id), "OK")
        self.assertEqual(a.call("CLUSTER", "SETSLOT", slot, "NODE", b_id), "OK")
        self.assertEqual(b.call("GET", key), b"v")
        self.assertEqual(str(a.call("GET", key)), "MOVED %d 127.0.0.1:%d" % (slot, b.port))
        # The target's bumped epoch wins everywhere.
        wait_until(lambda: a.owner_of(slot) == b_id and b.owner_of(slot) == b_id, 10, "slot handover")
        self.wait_converged([a, b])

    def test_live_resharding_under_load(self):
        a, b, c = self.cluster(3)
        client = ClusterClient(a.addr)
        expected = {}
        for i in range(2000):
            k = "user:%d" % i
            client.call("SET", k, "v0-%d" % i)
            expected[k] = "v0-%d" % i

        # A writer keeps updating and reading keys through the redirects
        # while slots move underneath it.
        stop = threading.Event()
        errors = []
        lock = threading.Lock()

        def load():
            cc = ClusterClient(a.addr)
            rng = random.Random(1)
            n = 0
            try:
                while not stop.is_set():
                    k = "user:%d" % rng.randrange(2000)
                    n += 1
                    val = "v%d" % n
                    cc.call("SET", k, val)
                    got = cc.call("GET", k)
                    with lock:
                        expected[k] = val
                    if got != val.encode():
                        errors.append((k, val, got))
            except Exception as e:  # noqa: BLE001
                errors.append(repr(e))
            finally:
                self.load_redirects = cc.redirects
                cc.close()

        t = threading.Thread(target=load)
        t.start()
        try:
            a_id = a.myid()
            a_slots = sorted(s for x in a.nodes() if x["id"] == a_id for s in x["slots"])
            moved_slots, moved_keys = kv_cluster.reshard(a.addr, b.addr, slots=a_slots[:600], batch=10)
            self.assertEqual(moved_slots, 600)
            self.assertGreater(moved_keys, 0)
        finally:
            time.sleep(0.3)
            stop.set()
            t.join(30)
        self.assertEqual(errors, [])
        self.assertGreater(sum(self.load_redirects.values()), 0)

        self.wait_converged([a, b, c])
        b_id = b.myid()
        for n in (a, b, c):
            self.assertTrue(all(n.owner_of(s) == b_id for s in a_slots[:600:50]))
        # Every key is still there, exactly once, with its latest value.
        check = ClusterClient(c.addr)
        for k, v in expected.items():
            self.assertEqual(check.call("GET", k), v.encode(), k)
        self.assertEqual(sum(n.call("DBSIZE") for n in (a, b, c)), 2000)
        check.close()
        client.close()

    def test_higher_config_epoch_wins_a_slot_conflict(self):
        a, b = self.node(), self.node()
        a.call("CLUSTER", "SET-CONFIG-EPOCH", 1)
        a.call("CLUSTER", "ADDSLOTSRANGE", 0, 16383)
        key = key_in_slot_range(a, 0, 99)
        a.call("SET", key, "stale")
        # b claims 0-99 too, under a higher epoch.
        b.call("CLUSTER", "SET-CONFIG-EPOCH", 5)
        b.call("CLUSTER", "ADDSLOTSRANGE", 0, 99)
        a.call("CLUSTER", "MEET", "127.0.0.1", b.port, b.cport)

        b_id = b.myid()
        a_id = a.myid()
        wait_until(lambda: a.owner_of(50) == b_id, 10, "a accepting b's claim")
        wait_until(lambda: b.owner_of(100) == a_id, 10, "b learning a's remaining slots")
        self.assertEqual(a.owner_of(100), a_id)
        self.assertEqual(b.owner_of(50), b_id)
        # a lost the slot, so its now-unreachable copy of the key is gone.
        wait_until(lambda: a.call("DBSIZE") == 0, 5, "a dropping keys of lost slots")
        self.wait_converged([a, b])

    def test_equal_config_epochs_get_resolved(self):
        nodes = [self.node() for _ in range(3)]
        for n in nodes[1:]:
            n.call("CLUSTER", "MEET", "127.0.0.1", nodes[0].port, nodes[0].cport)
        # All start at epoch 0; the collision rule must make them distinct.
        wait_until(lambda: len({x["epoch"] for x in nodes[0].nodes()}) == 3, 15, "distinct epochs")
        epochs = {n.myid(): next(x["epoch"] for x in n.nodes() if "myself" in x["flags"]) for n in nodes}
        wait_until(lambda: all({x["id"]: x["epoch"] for x in n.nodes()} == epochs for n in nodes), 15,
                   "agreement on epochs")

    def test_restart_keeps_identity_slots_and_data(self):
        a, b, c = self.cluster(3)
        client = ClusterClient(a.addr)
        for i in range(200):
            client.call("SET", "k%d" % i, str(i))
        client.close()
        b_id, b_count = b.myid(), b.call("DBSIZE")
        self.assertEqual(b.call("SAVE"), "OK")

        self.assertEqual(b.terminate(), 0)
        self.all_nodes.remove(b)
        b2 = self.node(data_dir=b.dir, port=b.port, cport=b.cport)
        self.assertEqual(b2.myid(), b_id)
        self.assertEqual(b2.call("DBSIZE"), b_count)
        self.wait_converged([a, b2, c])
        client = ClusterClient(c.addr)
        for i in range(200):
            self.assertEqual(client.call("GET", "k%d" % i), str(i).encode())
        client.close()

    def test_replicas(self):
        nodes = self.cluster(2, replicas=1)
        m1, m2, r1, r2 = nodes
        m1_id = m1.myid()
        r1_view = next(x for x in r1.nodes() if "myself" in x["flags"])
        self.assertIn("slave", r1_view["flags"])
        self.assertEqual(r1_view["master"], m1_id)

        key = key_in_slot_range(m1, 0, 8191)
        slot = m1.call("CLUSTER", "KEYSLOT", key)
        self.assertEqual(m1.call("SET", key, "v"), "OK")
        wait_until(lambda: r1.call("DBSIZE") == 1, 10, "replication to r1")

        self.assertEqual(str(r1.call("GET", key)), "MOVED %d 127.0.0.1:%d" % (slot, m1.port))
        c = r1.conn()
        self.assertEqual(c.call("READONLY"), "OK")
        self.assertEqual(c.call("GET", key), b"v")
        self.assertEqual(str(c.call_raw("SET", key, "x")), "MOVED %d 127.0.0.1:%d" % (slot, m1.port))
        c.close()
        # Replicas show up in CLUSTER SLOTS next to their master.
        slots = m2.call("CLUSTER", "SLOTS")
        entry = next(e for e in slots if e[0] <= slot <= e[1])
        self.assertEqual(entry[2][1], m1.port)
        self.assertEqual(entry[3][1], r1.port)
        self.assertEqual(r1.call("REPLICAOF", "NO", "ONE").args[0], "ERR REPLICAOF not allowed in cluster mode.")

    def test_forget(self):
        a, b = self.cluster(2)
        extra = self.node()
        extra.call("CLUSTER", "MEET", "127.0.0.1", a.port, a.cport)
        extra_id = extra.myid()
        wait_until(lambda: all(extra_id in [x["id"] for x in n.nodes()] for n in (a, b)), 10,
                   "a and b learning about the new node")
        extra.kill()
        for n in (a, b):
            self.assertEqual(n.call("CLUSTER", "FORGET", extra_id), "OK")
        time.sleep(3)  # several gossip rounds: nobody may re-learn it
        for n in (a, b):
            self.assertNotIn(extra_id, [x["id"] for x in n.nodes()])
        self.wait_converged([a, b])

    # --- failover -------------------------------------------------------------

    def test_automatic_failover_then_old_master_rejoins_as_replica(self):
        m1, m2, m3, r1, r2, r3 = self.cluster(3, replicas=1)
        m1_id, r1_id = m1.myid(), r1.myid()
        self.assertEqual(r1.me()["master"], m1_id)
        m1_slots = sorted(m1.me()["slots"])
        epoch_before = max(x["epoch"] for x in m2.nodes())

        client = ClusterClient(m2.addr)
        for i in range(300):
            client.call("SET", "key:%d" % i, "v%d" % i)
        self.wait_replicated(m1, r1)

        started = time.time()
        m1.kill()
        survivors = [m2, m3, r1, r2, r3]
        # Failure detected, agreed on by a majority, election won, new
        # config spread: every survivor sees r1 owning m1's old slots.
        wait_until(lambda: all(n.owner_of(m1_slots[0]) == r1_id for n in survivors), 30, "r1 taking over")
        took = time.time() - started
        self.wait_serving(survivors)

        me = r1.me()
        self.assertIn("master", me["flags"])
        self.assertEqual(sorted(me["slots"]), m1_slots)
        self.assertGreater(me["epoch"], epoch_before)  # won with a fresh, highest epoch
        for n in (m2, m3, r2, r3):
            self.assertIn("fail", n.view_of(m1_id)["flags"])
        self.assertEqual(r1.info("replication")["role"], "master")
        # node_timeout is 2s: detection ~2-3s, then a 0.5-1s election delay.
        self.assertLess(took, 15)

        # Nothing that had reached the replica was lost, and m1's slots
        # take writes again.
        for i in range(300):
            self.assertEqual(client.call("GET", "key:%d" % i), b"v%d" % i)
        after = keys_in_slots(set(m1_slots), 20, prefix="after")
        for k in after:
            self.assertEqual(client.call("SET", k, "new"), "OK")
        client.close()

        # m1 comes back still believing it owns its slots, under an older
        # epoch. It must learn it was replaced and turn into r1's replica.
        m1b = self.node(data_dir=m1.dir, port=m1.port, cport=m1.cport)
        wait_until(lambda: m1b.me()["master"] == r1_id and "slave" in m1b.me()["flags"], 20,
                   "the old master becoming a replica of the new one")
        wait_until(lambda: all(m1b.get_readonly(k) == b"new" for k in after), 20, "m1 resyncing from r1")
        self.assertEqual(m1b.call("DBSIZE"), r1.call("DBSIZE"))
        # Back and serving nothing: nobody holds its FAIL flag against it.
        wait_until(lambda: all("fail" not in n.view_of(m1_id)["flags"] for n in survivors), 20,
                   "the FAIL flag being cleared")
        self.wait_converged(survivors + [m1b])

    def test_manual_failover_loses_no_acknowledged_writes(self):
        m1, m2, m3, r1, r2, r3 = self.cluster(3, replicas=1)
        m1_id, r1_id = m1.myid(), r1.myid()
        keys = keys_in_slots(set(m1.me()["slots"]), 50, prefix="mf")

        # A writer hammers m1's slots through the whole failover. Every write
        # it saw acknowledged must survive: the master pauses writes, the
        # replica catches up to the master's final offset, and only then
        # takes over. Writes sent during the pause are held, then redirected.
        stop = threading.Event()
        acked = {}
        errors = []
        progress = {"after": 0}
        promoted = threading.Event()

        def writer():
            cc = ClusterClient(m2.addr)
            n = 0
            try:
                while not stop.is_set():
                    n += 1
                    k = keys[n % len(keys)]
                    if cc.call("SET", k, "v%d" % n) != "OK":
                        errors.append((k, n))
                    acked[k] = "v%d" % n
                    if promoted.is_set():
                        progress["after"] += 1
            except Exception as e:  # noqa: BLE001
                errors.append(repr(e))
            finally:
                cc.close()

        t = threading.Thread(target=writer)
        t.start()
        try:
            time.sleep(0.5)
            self.assertEqual(r1.call("CLUSTER", "FAILOVER"), "OK")
            wait_until(lambda: "master" in r1.me()["flags"], 10, "r1 promoted")
            promoted.set()
            wait_until(lambda: m1.me()["master"] == r1_id, 10, "m1 demoted to r1's replica")
            time.sleep(0.5)
        finally:
            stop.set()
            t.join(30)
        self.assertEqual(errors, [])
        self.assertGreater(progress["after"], 0)  # writes kept flowing to the new master

        check = ClusterClient(m3.addr)
        for k, v in acked.items():
            self.assertEqual(check.call("GET", k), v.encode(), k)
        check.close()
        self.wait_replicated(r1, m1)
        self.wait_converged([m1, m2, m3, r1, r2, r3])
        self.assertEqual(m1.view_of(m1_id)["master"], r1_id)

    def test_failover_force_without_waiting_for_fail(self):
        m1, m2, m3, r1, r2, r3 = self.cluster(3, replicas=1)
        r1_id = r1.myid()
        slot = min(m1.me()["slots"])
        m1.kill()
        # Right away, long before m1 could be flagged FAIL: the masters vote
        # anyway (FORCEACK), so the outage is just the election.
        self.assertEqual(r1.call("CLUSTER", "FAILOVER", "FORCE"), "OK")
        wait_until(lambda: all(n.owner_of(slot) == r1_id for n in (m2, m3, r1)), 10, "forced failover")
        self.wait_serving([m2, m3, r1, r2, r3])

    def test_failover_takeover_needs_no_vote(self):
        m1, m2, m3, r1, r2, r3 = self.cluster(3, replicas=1)
        m2_id, r2_id = m2.myid(), r2.myid()
        slot = min(m2.me()["slots"])
        # Two of three masters gone: no election could reach a majority.
        m1.kill()
        m3.kill()
        self.assertEqual(r2.call("CLUSTER", "FAILOVER", "TAKEOVER"), "OK")
        me = r2.me()
        self.assertIn("master", me["flags"])
        self.assertIn(slot, me["slots"])
        # m2 was alive: it sees the higher-epoch claim and steps down.
        wait_until(lambda: m2.me()["master"] == r2_id, 10, "m2 stepping down")
        wait_until(lambda: r2.view_of(m2_id)["master"] == r2_id, 10, "r2 learning m2 is now its replica")

    def test_master_in_minority_stops_accepting_writes(self):
        a, b, c = self.cluster(3)
        key = key_in_slot_range(a, min(a.me()["slots"]), max(a.me()["slots"]))
        self.assertEqual(a.call("SET", key, "before"), "OK")
        b.kill()
        c.kill()
        # a can't reach a majority of masters: the others may be failing
        # over its slots right now, so accepting a write could lose it.
        wait_until(lambda: a.cluster_info()["cluster_state"] == "fail", 10, "a noticing it's alone")
        self.assertEqual(str(a.call("SET", key, "during")), "CLUSTERDOWN The cluster is down")
        self.assertEqual(str(a.call("GET", key)), "CLUSTERDOWN The cluster is down")

    def test_other_replicas_follow_the_new_master(self):
        m1, m2, m3 = self.cluster(3)
        ra = self.add_replica(m1, [m1, m2, m3])
        rb = self.add_replica(m1, [m1, m2, m3, ra])
        everyone = [m1, m2, m3, ra, rb]
        self.wait_converged(everyone)
        m1_slots = set(m1.me()["slots"])

        client = ClusterClient(m2.addr)
        for k in keys_in_slots(m1_slots, 100, prefix="pre"):
            client.call("SET", k, "x")
        self.wait_replicated(m1, ra)
        self.wait_replicated(m1, rb)

        m1.kill()
        slot = min(m1_slots)
        ids = {ra.myid(): ra, rb.myid(): rb}
        wait_until(lambda: m2.owner_of(slot) in ids, 30, "one replica winning the election")
        winner = ids[m2.owner_of(slot)]
        loser = ra if winner is rb else rb
        winner_id = winner.myid()
        # The loser's master was replaced: it follows the winner, and can
        # continue its old replication stream there (PSYNC with the
        # winner's previous replication ID) instead of a full resync.
        wait_until(lambda: loser.me()["master"] == winner_id, 20, "the other replica following the winner")
        post = keys_in_slots(m1_slots, 10, prefix="post")
        for k in post:
            client.call("SET", k, "y")
        wait_until(lambda: all(loser.get_readonly(k) == b"y" for k in post), 20, "replication from the winner")
        self.assertGreaterEqual(int(winner.info("replication")["sync_partial_ok"]), 1)
        client.close()
        self.wait_serving([m2, m3, ra, rb])


if __name__ == "__main__":
    if len(sys.argv) > 1:
        test_server.SERVER_BIN = os.path.abspath(sys.argv.pop(1))
    unittest.main(verbosity=2)
