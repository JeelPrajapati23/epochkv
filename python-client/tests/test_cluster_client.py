"""ClusterClient against scripted fake nodes: each redirect and failure case
on demand, without a real cluster.

Usage: python3 python-client/tests/test_cluster_client.py
"""

import os
import socket
import sys
import threading
import time
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from kvclient.cluster import (  # noqa: E402
    SLOTS, ClusterClient, CrossSlotError, UncertainWriteError, command_keys, crc16, key_slot)
from kvclient.resp import Conn, ReplyError  # noqa: E402

CLOSE = object()  # a handler's reply that drops the connection instead


def resp(value):
    if isinstance(value, ReplyError):
        return b"-%s\r\n" % str(value).encode()
    if isinstance(value, str):
        return b"+%s\r\n" % value.encode()
    if isinstance(value, int):
        return b":%d\r\n" % value
    if isinstance(value, bytes):
        return b"$%d\r\n%s\r\n" % (len(value), value)
    if value is None:
        return b"$-1\r\n"
    return b"*%d\r\n" % len(value) + b"".join(resp(v) for v in value)


class FakeNode:
    """A TCP server speaking just enough RESP. Every command is logged and
    passed to `handler(args)`, which returns the reply (or CLOSE); CLUSTER
    SLOTS answers with `slot_map`. The connection is closed right after
    replying to a command for which `close_after(args)` is true."""

    def __init__(self):
        self.listener = socket.create_server(("127.0.0.1", 0))
        self.addr = ("127.0.0.1", self.listener.getsockname()[1])
        self.log = []
        self.slot_map = []
        self.handler = lambda args: "OK"
        self.close_after = lambda args: False
        threading.Thread(target=self._serve, daemon=True).start()

    def commands(self, name):
        return [a for a in self.log if a[0] == name]

    def _serve(self):
        while True:
            try:
                sock, _ = self.listener.accept()
            except OSError:
                return
            threading.Thread(target=self._client, args=(sock,), daemon=True).start()

    def _client(self, sock):
        # Reuse Conn's reply parser: a command is just an array of bulk strings.
        c = Conn.__new__(Conn)
        c.sock, c.buf, c.host, c.port = sock, b"", "client", 0
        with sock:
            while True:
                try:
                    args = [a.decode() for a in c.read_reply()]
                except (ConnectionError, OSError):
                    return
                self.log.append(tuple(args))
                if args[0] == "CLUSTER" and args[1] == "SLOTS":
                    reply = self.slot_map
                elif args[0] == "ASKING":
                    reply = "OK"
                else:
                    reply = self.handler(args)
                if reply is CLOSE:
                    return
                sock.sendall(resp(reply))
                if self.close_after(args):
                    return

    def stop(self):
        # On Linux, close() alone doesn't stop a listener another thread is
        # blocked in accept() on: it keeps accepting. shutdown() wakes it.
        try:
            self.listener.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass  # Windows refuses shutdown() on a listener; close() suffices
        self.listener.close()


def owns(node, lo=0, hi=SLOTS - 1):
    return [lo, hi, [node.addr[0].encode(), node.addr[1], b"id"]]


def key_in_slot_of(other):
    """A key whose slot differs from `other`'s."""
    return next(k for k in ("k%d" % i for i in range(100)) if key_slot(k) != key_slot(other))


class TestHashing(unittest.TestCase):
    def test_known_values(self):
        self.assertEqual(crc16(b"123456789"), 0x31C3)  # CRC16/XMODEM check value
        self.assertEqual(key_slot("foo"), 12182)  # same as real Redis

    def test_hash_tags(self):
        self.assertEqual(key_slot("{user1000}.following"), key_slot("{user1000}.followers"))
        self.assertEqual(key_slot("foo{}{bar}"), key_slot(b"foo{}{bar}"))  # empty tag: whole key
        self.assertEqual(key_slot("foo{{bar}}zap"), key_slot("{bar"))  # first '{' to next '}'

    def test_command_keys(self):
        self.assertEqual(command_keys(("DEL", "a", "b", "c")), (["a", "b", "c"], True))
        self.assertEqual(command_keys(("set", "k", "v", "EX", "10")), (["k"], True))
        self.assertEqual(command_keys(("GET", "k")), (["k"], False))
        self.assertEqual(command_keys(("PING",)), ([], False))
        self.assertEqual(command_keys((b"INCR", "x")), ([], True))  # unknown: a write


class TestClusterClient(unittest.TestCase):
    def setUp(self):
        self.a, self.b = FakeNode(), FakeNode()
        self.a.slot_map = self.b.slot_map = [owns(self.a)]
        self.client = ClusterClient(self.a.addr, retry_timeout=1)

    def tearDown(self):
        self.client.close()
        self.a.stop()
        self.b.stop()

    def test_routes_to_owner(self):
        self.a.slot_map = [owns(self.a, 0, 8191), owns(self.b, 8192, SLOTS - 1)]
        self.client.refresh()
        low = next(k for k in ("k%d" % i for i in range(100)) if key_slot(k) < 8192)
        high = next(k for k in ("k%d" % i for i in range(100)) if key_slot(k) >= 8192)
        self.client.call("SET", low, "1")
        self.client.call("SET", high, "2")
        self.assertEqual(self.a.commands("SET"), [("SET", low, "1")])
        self.assertEqual(self.b.commands("SET"), [("SET", high, "2")])

    def test_keyless_command(self):
        self.a.handler = lambda args: "PONG"
        self.assertEqual(self.client.call("PING"), "PONG")

    def test_cross_slot_fails_locally(self):
        other = key_in_slot_of("a")
        with self.assertRaises(CrossSlotError):
            self.client.call("DEL", "a", other)
        self.assertEqual(self.a.commands("DEL"), [])  # never sent
        self.client.call("DEL", "{a}x", "{a}y")  # same tag, same slot: fine

    def test_moved_updates_map(self):
        slot = key_slot("k")
        self.a.handler = lambda args: ReplyError("MOVED %d 127.0.0.1:%d" % (slot, self.b.addr[1]))
        self.b.handler = lambda args: b"v"
        self.assertEqual(self.client.call("GET", "k"), b"v")
        self.assertEqual(self.client.call("GET", "k"), b"v")
        self.assertEqual(len(self.a.commands("GET")), 1)  # second GET went straight to b
        self.assertEqual(self.client.redirects["MOVED"], 1)
        self.assertEqual(self.client.slots[slot], self.b.addr)

    def test_ask_is_one_shot(self):
        slot = key_slot("k")
        self.a.handler = lambda args: ReplyError("ASK %d 127.0.0.1:%d" % (slot, self.b.addr[1]))
        self.b.handler = lambda args: b"v"
        self.assertEqual(self.client.call("GET", "k"), b"v")
        self.assertEqual(self.b.log, [("ASKING",), ("GET", "k")])  # ASKING right before it
        self.assertEqual(self.client.slots[slot], self.a.addr)  # map unchanged
        self.client.call("GET", "k")
        self.assertEqual(len(self.a.commands("GET")), 2)  # next GET asks the owner again

    def test_too_many_redirects(self):
        slot = key_slot("k")
        self.a.handler = lambda args: ReplyError("MOVED %d 127.0.0.1:%d" % (slot, self.b.addr[1]))
        self.b.handler = lambda args: ReplyError("MOVED %d 127.0.0.1:%d" % (slot, self.a.addr[1]))
        with self.assertRaisesRegex(ReplyError, "too many redirects"):
            self.client.call("GET", "k")

    def test_other_errors_raise(self):
        self.a.handler = lambda args: ReplyError("ERR wrong number of arguments")
        with self.assertRaisesRegex(ReplyError, "wrong number"):
            self.client.call("GET", "k")

    def test_write_lost_mid_flight_is_not_retried(self):
        self.a.handler = lambda args: CLOSE  # reads the SET, then dies
        with self.assertRaises(UncertainWriteError):
            self.client.call("SET", "k", "v")
        self.assertEqual(len(self.a.commands("SET")), 1)  # delivered exactly once

    def test_read_lost_mid_flight_is_retried(self):
        replies = [CLOSE, b"v"]
        self.a.handler = lambda args: replies.pop(0)
        self.assertEqual(self.client.call("GET", "k"), b"v")
        self.assertEqual(len(self.a.commands("GET")), 2)

    def test_idle_connection_closed_by_peer_is_replaced_before_sending(self):
        self.a.close_after = lambda args: args == ["SET", "k", "2"]
        self.client.call("SET", "k", "1")
        self.client.call("SET", "k", "2")  # its reply is the connection's last
        time.sleep(0.1)  # the FIN arrives while the client is idle
        self.assertEqual(self.client.call("SET", "k", "3"), "OK")  # no UncertainWriteError
        self.assertEqual(len(self.a.commands("SET")), 3)

    def test_clusterdown_retries_then_succeeds(self):
        replies = [ReplyError("CLUSTERDOWN The cluster is down"), "OK"]
        self.a.handler = lambda args: replies.pop(0)
        self.assertEqual(self.client.call("SET", "k", "v"), "OK")  # safe: it didn't run
        self.assertEqual(self.client.retries["CLUSTERDOWN"], 1)

    def test_dead_node_fails_over_to_new_owner(self):
        # b is a's replica (so the client knows it), then takes a's slots.
        self.a.slot_map = [[0, SLOTS - 1, [b"127.0.0.1", self.a.addr[1], b"a"], [b"127.0.0.1", self.b.addr[1], b"b"]]]
        self.client.refresh()
        self.client.retry_timeout = 10  # a refused connect takes ~2s on Windows
        self.b.slot_map = [owns(self.b)]
        self.b.handler = lambda args: "OK"
        self.client.close()
        self.a.stop()  # a is gone: connect fails, so nothing was sent
        self.assertEqual(self.client.call("SET", "k", "v"), "OK")
        self.assertEqual(self.b.commands("SET"), [("SET", "k", "v")])
        self.assertEqual(self.client.slots[key_slot("k")], self.b.addr)


if __name__ == "__main__":
    unittest.main()
