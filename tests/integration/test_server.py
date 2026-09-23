"""End-to-end tests: start the real kv_server binary and talk RESP over TCP.

Usage: python3 tests/integration/test_server.py [path/to/kv_server]
(defaults to build/src/kv_server relative to the repo root)
"""

import os
import signal
import socket
import subprocess
import sys
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
        self.start_server()

    def start_server(self, *extra_args):
        """(Re)starts the server with extra command-line flags."""
        self.stop_server()
        self.port = free_port()
        self.proc = subprocess.Popen(
            [SERVER_BIN, "--port", str(self.port), *extra_args],
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

    def tearDown(self):
        self.stop_server()

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


if __name__ == "__main__":
    if len(sys.argv) > 1:
        SERVER_BIN = os.path.abspath(sys.argv.pop(1))
    unittest.main(verbosity=2)
