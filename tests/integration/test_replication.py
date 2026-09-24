"""End-to-end replication tests: several real kv_server processes on loopback.

Usage: python3 tests/integration/test_replication.py [path/to/kv_server]
"""

import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import test_server  # noqa: E402  (shared RESP client helpers)
from test_server import Client, free_port  # noqa: E402


class Node:
    """One kv_server process with its own data directory."""

    def __init__(self, *args, data_dir=None):
        self.dir = data_dir or tempfile.mkdtemp(prefix="kv_store_repl_")
        self.port = free_port()
        self.clients = []
        self.proc = subprocess.Popen(
            [test_server.SERVER_BIN, "--port", str(self.port), "--dir", self.dir, "--save", "", *args],
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

    def client(self):
        c = Client(self.port)
        self.clients.append(c)
        return c

    def info(self):
        c = Client(self.port)
        try:
            text = c.cmd("INFO", "replication").decode()
        finally:
            c.close()
        return dict(line.split(":", 1) for line in text.split("\r\n") if ":" in line)

    def kill(self):
        for c in self.clients:
            c.close()
        self.clients = []
        if self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait()

    def terminate(self):
        for c in self.clients:
            c.close()
        self.clients = []
        self.proc.send_signal(signal.SIGTERM)
        return self.proc.wait(timeout=10)


class Proxy:
    """TCP forwarder between a replica and its master that can cut the link,
    to simulate a network partition without restarting either process."""

    def __init__(self, target_port):
        self.target_port = target_port
        self.up = True
        self.lock = threading.Lock()
        self.socks = []
        self.listener = socket.socket()
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(16)
        self.port = self.listener.getsockname()[1]
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self):
        while True:
            try:
                downstream, _ = self.listener.accept()
            except OSError:
                return
            if not self.up:
                downstream.close()  # partitioned: refuse by hanging up at once
                continue
            try:
                upstream = socket.create_connection(("127.0.0.1", self.target_port))
            except OSError:
                downstream.close()
                continue
            with self.lock:
                self.socks += [downstream, upstream]
            threading.Thread(target=self._pump, args=(downstream, upstream), daemon=True).start()
            threading.Thread(target=self._pump, args=(upstream, downstream), daemon=True).start()

    @staticmethod
    def _pump(src, dst):
        try:
            while True:
                data = src.recv(65536)
                if not data:
                    break
                dst.sendall(data)
        except OSError:
            pass
        for s in (src, dst):
            try:
                s.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    def cut(self):
        self.up = False
        with self.lock:
            for s in self.socks:
                try:
                    s.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                s.close()
            self.socks = []

    def restore(self):
        self.up = True

    def close(self):
        self.cut()
        self.listener.close()


class ReplicationTest(unittest.TestCase):
    def setUp(self):
        self.nodes = []
        self.proxies = []

    def tearDown(self):
        for p in self.proxies:
            p.close()
        for n in self.nodes:
            n.kill()
            shutil.rmtree(n.dir, ignore_errors=True)

    def node(self, *args, **kwargs):
        n = Node(*args, **kwargs)
        self.nodes.append(n)
        return n

    def replica_of(self, master, *args, port=None):
        return self.node("--replicaof", "127.0.0.1", str(port or master.port), *args)

    def wait_until(self, predicate, timeout=10, msg="condition"):
        deadline = time.time() + timeout
        while not predicate():
            if time.time() > deadline:
                self.fail("%s not reached within %ss" % (msg, timeout))
            time.sleep(0.02)

    def wait_in_sync(self, master, replica):
        def synced():
            r = replica.info()
            return (r.get("master_link_status") == "up"
                    and r["slave_repl_offset"] == master.info()["master_repl_offset"])
        self.wait_until(synced, msg="replica in sync")

    # --- tests ---------------------------------------------------------------

    def test_full_sync_then_live_stream(self):
        master = self.node()
        m = master.client()
        for i in range(200):
            m.cmd("SET", "k%d" % i, "v%d" % i)
        m.cmd("SET", "ttl", "v", "EX", "100")

        replica = self.replica_of(master)
        self.wait_in_sync(master, replica)
        r = replica.client()
        self.assertEqual(r.cmd("DBSIZE"), 201)
        self.assertEqual(r.cmd("GET", "k199"), b"v199")
        self.assertTrue(95 <= r.cmd("TTL", "ttl") <= 100)

        # Changes after the sync arrive through the stream.
        m.cmd("SET", "new", "x")
        m.cmd("DEL", "k0")
        m.cmd("EXPIRE", "k1", "50")
        m.cmd("PERSIST", "ttl")
        self.wait_in_sync(master, replica)
        self.assertEqual(r.cmd("GET", "new"), b"x")
        self.assertIsNone(r.cmd("GET", "k0"))
        self.assertTrue(45 <= r.cmd("TTL", "k1") <= 50)
        self.assertEqual(r.cmd("TTL", "ttl"), -1)

        info = master.info()
        self.assertEqual(info["connected_slaves"], "1")
        self.assertIn("state=online", info["slave0"])
        self.assertIn("port=%d" % replica.port, info["slave0"])
        self.assertEqual(info["sync_full"], "1")

    def test_large_dataset_with_writes_during_the_transfer(self):
        master = self.node()
        m = master.client()
        value = b"x" * 1024
        for i in range(3000):  # ~3MB snapshot: many chunks, many EPOLLOUT rounds
            m.send(test_server.encode("SET", "big%d" % i, value))
        for _ in range(3000):
            m.read_reply()

        replica = self.replica_of(master)
        # Keep writing while the replica syncs: these land in the held
        # stream and must be applied after the snapshot, in order.
        for i in range(500):
            m.cmd("SET", "during%d" % i, str(i))
        self.wait_in_sync(master, replica)
        r = replica.client()
        self.assertEqual(r.cmd("DBSIZE"), 3500)
        self.assertEqual(r.cmd("GET", "big2999"), value)
        self.assertEqual(r.cmd("GET", "during499"), b"499")

    def test_replica_is_read_only(self):
        master = self.node()
        replica = self.replica_of(master)
        self.wait_in_sync(master, replica)
        r = replica.client()
        reply = r.cmd("SET", "k", "v")
        self.assertIsInstance(reply, RuntimeError)
        self.assertIn("READONLY", str(reply))
        self.assertIn("READONLY", str(r.cmd("DEL", "k")))
        self.assertEqual(r.cmd("PING"), "PONG")

    def test_replicaof_at_runtime_replaces_the_dataset_and_no_one_promotes(self):
        master = self.node()
        master.client().cmd("SET", "from_master", "1")
        other = self.node()
        o = other.client()
        o.cmd("SET", "only_here", "1")

        self.assertEqual(o.cmd("REPLICAOF", "127.0.0.1", str(master.port)), "OK")
        self.wait_in_sync(master, other)
        self.assertIsNone(o.cmd("GET", "only_here"))
        self.assertEqual(o.cmd("GET", "from_master"), b"1")

        self.assertEqual(o.cmd("REPLICAOF", "NO", "ONE"), "OK")
        self.assertEqual(other.info()["role"], "master")
        self.assertEqual(o.cmd("SET", "mine", "1"), "OK")
        self.assertEqual(o.cmd("GET", "from_master"), b"1")
        # No longer following: the old master's writes stop arriving.
        master.client().cmd("SET", "after", "1")
        time.sleep(0.3)
        self.assertIsNone(o.cmd("GET", "after"))

    def test_link_drop_resumes_with_partial_resync(self):
        master = self.node()
        proxy = Proxy(master.port)
        self.proxies.append(proxy)
        replica = self.replica_of(master, port=proxy.port)
        m = master.client()
        m.cmd("SET", "before", "1")
        self.wait_in_sync(master, replica)

        proxy.cut()
        self.wait_until(lambda: replica.info()["master_link_status"] == "down", msg="link down")
        for i in range(50):
            m.cmd("SET", "during%d" % i, str(i))
        m.cmd("DEL", "before")
        proxy.restore()

        self.wait_in_sync(master, replica)
        r = replica.client()
        self.assertEqual(r.cmd("GET", "during49"), b"49")
        self.assertIsNone(r.cmd("GET", "before"))
        info = master.info()
        self.assertEqual(info["sync_full"], "1")  # only the initial one
        self.assertEqual(info["sync_partial_ok"], "1")

    def test_link_drop_longer_than_the_backlog_needs_a_full_resync(self):
        master = self.node("--repl-backlog-size", "1kb")
        proxy = Proxy(master.port)
        self.proxies.append(proxy)
        replica = self.replica_of(master, port=proxy.port)
        m = master.client()
        self.wait_in_sync(master, replica)

        proxy.cut()
        self.wait_until(lambda: replica.info()["master_link_status"] == "down", msg="link down")
        for i in range(100):  # ~10KB of stream: far past the 1KB backlog
            m.cmd("SET", "k%d" % i, "v" * 64)
        proxy.restore()

        self.wait_in_sync(master, replica)
        self.assertEqual(replica.client().cmd("GET", "k99"), b"v" * 64)
        info = master.info()
        self.assertEqual(info["sync_full"], "2")
        self.assertEqual(info["sync_partial_err"], "1")

    def test_failover_to_a_replica_keeps_the_other_replica_partial(self):
        master = self.node()
        r1 = self.replica_of(master)
        r2 = self.replica_of(master)
        m = master.client()
        for i in range(20):
            m.cmd("SET", "k%d" % i, str(i))
        # Both at the same offset at the same moment (a master PING between
        # two separate checks could leave r1 one command behind r2).
        self.wait_until(lambda: r1.info()["slave_repl_offset"] == r2.info()["slave_repl_offset"]
                        == master.info()["master_repl_offset"], msg="both replicas in sync")

        master.kill()
        c1 = r1.client()
        self.assertEqual(c1.cmd("REPLICAOF", "NO", "ONE"), "OK")
        c2 = r2.client()
        self.assertEqual(c2.cmd("REPLICAOF", "127.0.0.1", str(r1.port)), "OK")
        self.wait_in_sync(r1, r2)

        # r2 continued r1's old history (replid2) instead of a full copy.
        info = r1.info()
        self.assertEqual(info["sync_full"], "0")
        self.assertEqual(info["sync_partial_ok"], "1")
        self.assertEqual(r2.info()["master_replid"], info["master_replid"])

        self.assertEqual(c1.cmd("SET", "after_failover", "yes"), "OK")
        self.wait_in_sync(r1, r2)
        self.assertEqual(c2.cmd("GET", "after_failover"), b"yes")
        self.assertEqual(c2.cmd("GET", "k19"), b"19")

    def test_chained_replicas_get_the_masters_stream(self):
        master = self.node()
        middle = self.replica_of(master)
        self.wait_in_sync(master, middle)
        leaf = self.replica_of(middle)
        self.wait_in_sync(middle, leaf)

        master.client().cmd("SET", "k", "v")
        self.wait_in_sync(master, middle)
        self.wait_in_sync(middle, leaf)
        self.assertEqual(leaf.client().cmd("GET", "k"), b"v")
        # One history end to end: same replid, same offset.
        self.assertEqual(leaf.info()["master_replid"], master.info()["master_replid"])

    def test_wait_blocks_until_replicas_acknowledge(self):
        master = self.node()
        replica = self.replica_of(master)
        self.wait_in_sync(master, replica)
        m = master.client()
        m.cmd("SET", "k", "v")
        self.assertEqual(m.cmd("WAIT", "1", "5000"), 1)

        start = time.time()
        self.assertEqual(m.cmd("WAIT", "2", "300"), 1)  # only one replica exists
        self.assertGreaterEqual(time.time() - start, 0.25)

        # Commands pipelined behind a blocked WAIT run after it, in order.
        m.send(test_server.encode("WAIT", "2", "200") + test_server.encode("GET", "k"))
        self.assertEqual(m.read_reply(), 1)
        self.assertEqual(m.read_reply(), b"v")

        self.assertIn("WAIT cannot be used", str(replica.client().cmd("WAIT", "1", "10")))

    def test_expiry_is_driven_by_the_master(self):
        master = self.node()
        replica = self.replica_of(master)
        self.wait_in_sync(master, replica)
        m = master.client()
        m.cmd("SET", "short", "v", "PX", "200")
        self.wait_in_sync(master, replica)
        r = replica.client()
        self.assertEqual(r.cmd("GET", "short"), b"v")
        time.sleep(0.3)
        # Logically expired on the replica at once, whatever the master does...
        self.assertIsNone(r.cmd("GET", "short"))
        # ...and physically removed once the master's expiry sends a DEL.
        self.wait_until(lambda: r.cmd("DBSIZE") == 0, msg="replica reclaims the key")

    def test_replica_keeps_synced_data_in_its_own_aof(self):
        master = self.node()
        master.client().cmd("SET", "k", "v")
        replica = self.replica_of(master, "--appendonly", "yes")
        self.wait_in_sync(master, replica)
        master.client().cmd("SET", "streamed", "1")
        self.wait_in_sync(master, replica)
        self.assertEqual(replica.terminate(), 0)

        # Restart standalone: everything must come back from its own AOF.
        alone = self.node("--appendonly", "yes", data_dir=replica.dir)
        c = alone.client()
        self.assertEqual(c.cmd("GET", "k"), b"v")
        self.assertEqual(c.cmd("GET", "streamed"), b"1")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        test_server.SERVER_BIN = os.path.abspath(sys.argv.pop(1))
    unittest.main(verbosity=2)
