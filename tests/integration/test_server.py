"""End-to-end tests: start the real kv_server binary and talk RESP over TCP.

Usage: python3 tests/integration/test_server.py [path/to/kv_server]
(defaults to build/src/kv_server relative to the repo root)
"""

import glob
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SERVER_BIN = os.path.join(REPO_ROOT, "build", "src", "kv_server")


def encode(*parts):
    out = b"*%d\r\n" % len(parts)
    for p in parts:
        if isinstance(p, str):
            p = p.encode()
        out += b"$%d\r\n%s\r\n" % (len(p), p)
    return out


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Client:
    def __init__(self, port):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=5)
        self.buf = b""

    def send(self, data):
        self.sock.sendall(data)

    def _fill(self):
        chunk = self.sock.recv(65536)
        if not chunk:
            raise ConnectionError("server closed connection")
        self.buf += chunk

    def _line(self):
        while b"\r\n" not in self.buf:
            self._fill()
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def read_reply(self):
        line = self._line()
        kind, rest = line[:1], line[1:]
        if kind == b"+":
            return rest.decode()
        if kind == b"-":
            return RuntimeError(rest.decode())
        if kind == b":":
            return int(rest)
        if kind == b"$":
            n = int(rest)
            if n == -1:
                return None
            while len(self.buf) < n + 2:
                self._fill()
            data, self.buf = self.buf[:n], self.buf[n + 2:]
            return data
        if kind == b"*":
            n = int(rest)
            return None if n == -1 else [self.read_reply() for _ in range(n)]
        raise ValueError("unexpected reply: %r" % line)

    def cmd(self, *parts):
        self.send(encode(*parts))
        return self.read_reply()

    def is_closed(self):
        try:
            return self.sock.recv(1) == b""
        except ConnectionResetError:
            return True

    def close(self):
        self.sock.close()


class ServerTest(unittest.TestCase):
    def setUp(self):
        self.clients = []
        self.proc = None
        # Each test gets its own data directory, kept across restarts within
        # the test, so snapshots and AOFs never leak between tests.
        self.dir = tempfile.mkdtemp(prefix="kv_store_it_")
        self.start_server()

    def start_server(self, *extra_args):
        """(Re)starts the server with extra command-line flags."""
        self.stop_server()
        self.port = free_port()
        self.proc = subprocess.Popen(
            [SERVER_BIN, "--port", str(self.port), "--dir", self.dir, *extra_args],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = time.time() + 5
        while True:
            try:
                socket.create_connection(("127.0.0.1", self.port), timeout=0.2).close()
                break
            except OSError:
                if time.time() > deadline:
                    raise
                time.sleep(0.02)

    def stop_server(self):
        for c in self.clients:
            c.close()
        self.clients = []
        if self.proc is not None and self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait()

    def shutdown_server(self):
        """Graceful stop (SIGTERM), which runs the shutdown persistence path."""
        for c in self.clients:
            c.close()
        self.clients = []
        self.proc.send_signal(signal.SIGTERM)
        self.assertEqual(self.proc.wait(timeout=10), 0)

    def tearDown(self):
        self.stop_server()
        shutil.rmtree(self.dir, ignore_errors=True)

    def wait_until(self, predicate, timeout=10):
        deadline = time.time() + timeout
        while not predicate():
            if time.time() > deadline:
                self.fail("condition not reached within %ss" % timeout)
            time.sleep(0.02)

    def client(self):
        c = Client(self.port)
        self.clients.append(c)
        return c

    def test_basic_commands(self):
        c = self.client()
        self.assertEqual(c.cmd("PING"), "PONG")
        self.assertEqual(c.cmd("SET", "k", "v"), "OK")
        self.assertEqual(c.cmd("GET", "k"), b"v")
        self.assertEqual(c.cmd("DEL", "k"), 1)
        self.assertIsNone(c.cmd("GET", "k"))

    def test_concurrent_clients_are_served_interleaved(self):
        # The blocking server would hang on b's first command until a left.
        a, b = self.client(), self.client()
        self.assertEqual(a.cmd("SET", "shared", "1"), "OK")
        self.assertEqual(b.cmd("GET", "shared"), b"1")
        self.assertEqual(b.cmd("SET", "shared", "2"), "OK")
        self.assertEqual(a.cmd("GET", "shared"), b"2")

    def test_many_clients(self):
        clients = [self.client() for _ in range(200)]
        for i, c in enumerate(clients):
            c.send(encode("SET", "key%d" % i, "val%d" % i))
        for c in clients:
            self.assertEqual(c.read_reply(), "OK")
        for i, c in enumerate(clients):
            self.assertEqual(c.cmd("GET", "key%d" % i), ("val%d" % i).encode())

    def test_idle_client_does_not_block_others(self):
        idle = self.client()
        idle.send(b"*2\r\n$3\r\nGET\r\n")  # half a command, never finished
        active = self.client()
        self.assertEqual(active.cmd("PING"), "PONG")

    def test_pipelined_commands_reply_in_order(self):
        c = self.client()
        c.send(encode("SET", "a", "1") + encode("GET", "a") + encode("GET", "zz") + encode("PING"))
        self.assertEqual([c.read_reply() for _ in range(4)], ["OK", b"1", None, "PONG"])

    def test_command_split_across_sends(self):
        c = self.client()
        wire = encode("ECHO", "split")
        for byte in wire:
            c.send(bytes([byte]))
            time.sleep(0.001)
        self.assertEqual(c.read_reply(), b"split")

    def test_large_value_survives_partial_writes(self):
        # 8MB reply won't fit in the kernel send buffer, so the server must
        # hit EAGAIN, arm EPOLLOUT, and finish the write on later wakeups.
        value = os.urandom(8 * 1024 * 1024)
        writer = self.client()
        self.assertEqual(writer.cmd("SET", "big", value), "OK")
        reader = self.client()
        reader.send(encode("GET", "big"))
        time.sleep(0.2)  # let the server's send buffer fill up
        self.assertEqual(writer.cmd("PING"), "PONG")  # server not stuck on reader
        self.assertEqual(reader.read_reply(), value)

    def test_protocol_error_replies_then_closes(self):
        c = self.client()
        c.send(b"garbage\r\n")
        reply = c.read_reply()
        self.assertIsInstance(reply, RuntimeError)
        self.assertIn("Protocol error", str(reply))
        self.assertTrue(c.is_closed())
        self.assertEqual(self.client().cmd("PING"), "PONG")

    def test_ttl_expires_keys_lazily_and_actively(self):
        c = self.client()
        self.assertEqual(c.cmd("SET", "lazy", "v", "PX", "100"), "OK")
        for i in range(50):
            self.assertEqual(c.cmd("SET", "active%d" % i, "v", "PX", "100"), "OK")
        self.assertEqual(c.cmd("SET", "keep", "v"), "OK")
        self.assertEqual(c.cmd("DBSIZE"), 52)
        time.sleep(0.5)  # several 100ms cron ticks
        # The active sweep reclaimed the keys nobody touched...
        self.assertLessEqual(c.cmd("DBSIZE"), 2)
        # ...and any survivor is still invisible once its deadline has passed.
        self.assertIsNone(c.cmd("GET", "lazy"))
        self.assertEqual(c.cmd("DBSIZE"), 1)
        self.assertEqual(c.cmd("GET", "keep"), b"v")

    def test_allkeys_lru_eviction_under_maxmemory(self):
        self.start_server("--maxmemory", "100kb", "--maxmemory-policy", "allkeys-lru")
        c = self.client()
        value = b"x" * 1024
        for i in range(500):  # ~500KB of values into a 100KB budget
            self.assertEqual(c.cmd("SET", "k%d" % i, value), "OK")
            if i >= 10:
                c.cmd("GET", "k0")  # keep k0 hot
        self.assertLess(c.cmd("DBSIZE"), 100)
        self.assertEqual(c.cmd("GET", "k0"), value)
        self.assertIsNone(c.cmd("GET", "k1"))
        self.assertEqual(c.cmd("GET", "k499"), value)

    def test_noeviction_rejects_writes_with_oom(self):
        self.start_server("--maxmemory", "10kb")
        c = self.client()
        replies = [c.cmd("SET", "k%d" % i, b"x" * 1024) for i in range(20)]
        self.assertIsInstance(replies[-1], RuntimeError)
        self.assertIn("OOM", str(replies[-1]))
        self.assertEqual(c.cmd("GET", "k0"), b"x" * 1024)

    def test_sigterm_shuts_down_cleanly(self):
        self.client()
        self.proc.send_signal(signal.SIGTERM)
        self.assertEqual(self.proc.wait(timeout=5), 0)

    # --- persistence -------------------------------------------------------

    def aof_dir(self):
        return os.path.join(self.dir, "appendonlydir")

    def test_snapshot_on_shutdown_survives_restart(self):
        c = self.client()
        self.assertEqual(c.cmd("SET", "k", "v"), "OK")
        self.assertEqual(c.cmd("SET", "ttl", "v", "EX", "100"), "OK")
        self.shutdown_server()  # default save points => final SAVE on shutdown
        self.start_server()
        c = self.client()
        self.assertEqual(c.cmd("GET", "k"), b"v")
        self.assertTrue(95 <= c.cmd("TTL", "ttl") <= 100)

    def test_bgsave_survives_kill9(self):
        self.start_server("--save", "")
        c = self.client()
        self.assertEqual(c.cmd("SET", "k", "v"), "OK")
        self.assertEqual(c.cmd("BGSAVE"), "Background saving started")
        dump = os.path.join(self.dir, "dump.snap")
        self.wait_until(lambda: os.path.exists(dump))
        self.assertEqual(c.cmd("SET", "unsaved", "v"), "OK")
        self.stop_server()  # SIGKILL: no shutdown save
        self.start_server("--save", "")
        c = self.client()
        self.assertEqual(c.cmd("GET", "k"), b"v")
        self.assertIsNone(c.cmd("GET", "unsaved"))

    def test_aof_always_survives_kill9(self):
        flags = ("--save", "", "--appendonly", "yes", "--appendfsync", "always")
        self.start_server(*flags)
        c = self.client()
        self.assertEqual(c.cmd("SET", "a", "1"), "OK")
        self.assertEqual(c.cmd("SET", "b", "2", "PX", "60000"), "OK")
        self.assertEqual(c.cmd("DEL", "a"), 1)
        self.assertEqual(c.cmd("SET", "c", "3"), "OK")
        self.stop_server()  # SIGKILL right after the last OK
        self.start_server(*flags)
        c = self.client()
        self.assertIsNone(c.cmd("GET", "a"))
        self.assertEqual(c.cmd("GET", "b"), b"2")
        self.assertTrue(0 < c.cmd("PTTL", "b") <= 60000)
        self.assertEqual(c.cmd("GET", "c"), b"3")

    def test_sigterm_with_everysec_aof_flushes_and_exits_cleanly(self):
        # everysec starts a background fsync thread; SIGTERM must still reach
        # the signalfd instead of killing the process via that thread.
        flags = ("--save", "", "--appendonly", "yes", "--appendfsync", "everysec")
        self.start_server(*flags)
        self.assertEqual(self.client().cmd("SET", "k", "v"), "OK")
        self.shutdown_server()
        self.start_server(*flags)
        self.assertEqual(self.client().cmd("GET", "k"), b"v")

    def test_pipelined_writes_all_reach_the_aof(self):
        flags = ("--save", "", "--appendonly", "yes", "--appendfsync", "always")
        self.start_server(*flags)
        c = self.client()
        c.send(b"".join(encode("SET", "k%d" % i, "v%d" % i) for i in range(1000)))
        self.assertEqual([c.read_reply() for _ in range(1000)], ["OK"] * 1000)
        self.stop_server()
        self.start_server(*flags)
        self.assertEqual(self.client().cmd("DBSIZE"), 1000)

    def test_bgrewriteaof_compacts_and_keeps_data(self):
        flags = ("--save", "", "--appendonly", "yes")
        self.start_server(*flags)
        c = self.client()
        for i in range(2000):
            c.cmd("SET", "counter", str(i))
        self.assertEqual(c.cmd("BGREWRITEAOF"), "Background append only file rewriting started")

        def compacted():
            incrs = glob.glob(os.path.join(self.aof_dir(), "*.incr.aof"))
            bases = glob.glob(os.path.join(self.aof_dir(), "*.base.snap"))
            return len(incrs) == 1 and len(bases) == 1 and os.path.getsize(incrs[0]) == 0

        self.wait_until(compacted)
        self.assertEqual(c.cmd("SET", "after", "rewrite"), "OK")
        self.shutdown_server()
        self.start_server(*flags)
        c = self.client()
        self.assertEqual(c.cmd("GET", "counter"), b"1999")
        self.assertEqual(c.cmd("GET", "after"), b"rewrite")

    def test_aof_with_torn_last_command_still_loads(self):
        flags = ("--save", "", "--appendonly", "yes", "--appendfsync", "always")
        self.start_server(*flags)
        self.assertEqual(self.client().cmd("SET", "k", "v"), "OK")
        self.stop_server()
        (incr,) = glob.glob(os.path.join(self.aof_dir(), "*.incr.aof"))
        with open(incr, "ab") as f:
            f.write(b"*3\r\n$3\r\nSET\r\n$4\r\nhal")  # crash mid-write
        self.start_server(*flags)
        c = self.client()
        self.assertEqual(c.cmd("GET", "k"), b"v")
        self.assertEqual(c.cmd("DBSIZE"), 1)

    def test_corrupt_snapshot_refuses_to_start(self):
        self.shutdown_server()
        with open(os.path.join(self.dir, "dump.snap"), "wb") as f:
            f.write(b"definitely not a snapshot")
        proc = subprocess.Popen([SERVER_BIN, "--port", str(free_port()), "--dir", self.dir],
                                stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        _, err = proc.communicate(timeout=5)
        self.assertEqual(proc.returncode, 1)
        self.assertIn(b"bad magic", err)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        SERVER_BIN = os.path.abspath(sys.argv.pop(1))
    unittest.main(verbosity=2)
