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

    def test_standalone_server(self):
        self.a.slot_map = ReplyError("ERR This instance has cluster support disabled")
        self.client.refresh()
        self.assertEqual(set(self.client.slots), {self.a.addr})
        self.assertEqual(self.client.call("SET", "k", "v"), "OK")

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


class TestPipeline(unittest.TestCase):
    """a owns slots 0-8191, b owns 8192-16383."""

    def setUp(self):
        self.a, self.b = FakeNode(), FakeNode()
        self.a.slot_map = self.b.slot_map = [owns(self.a, 0, 8191), owns(self.b, 8192, SLOTS - 1)]
        for node in (self.a, self.b):
            node.handler = lambda args: b"v:" + args[1].encode() if args[0] == "GET" else "OK"
        self.client = ClusterClient(self.a.addr, retry_timeout=1)

    def tearDown(self):
        self.client.close()
        self.a.stop()
        self.b.stop()

    def keys_on(self, node, n, tag=None):
        """n keys owned by node (all in one slot if tag is given)."""
        lo = 0 if node is self.a else 8192
        if tag:
            tag = next(t for t in ("%s%d" % (tag, i) for i in range(1000)) if lo <= key_slot(t) < lo + 8192)
            return ["{%s}%d" % (tag, i) for i in range(n)]
        return [k for k in ("k%d" % i for i in range(100000)) if lo <= key_slot(k) < lo + 8192][:n]

    def test_results_come_back_in_queue_order_across_nodes(self):
        ka, kb = self.keys_on(self.a, 3), self.keys_on(self.b, 3)
        order = [ka[0], kb[0], kb[1], ka[1], ka[2], kb[2]]
        pipe = self.client.pipeline()
        for k in order:
            pipe.call("GET", k)
        self.assertEqual(pipe.execute(), [b"v:" + k.encode() for k in order])
        # Each node got only its own commands, in queue order.
        self.assertEqual(self.a.commands("GET"), [("GET", k) for k in order if k in ka])
        self.assertEqual(self.b.commands("GET"), [("GET", k) for k in order if k in kb])

    def test_moved_inside_a_batch_retries_only_that_command(self):
        k1, k2 = self.keys_on(self.a, 2)
        moved = key_slot(k1)
        self.a.handler = lambda args: (ReplyError("MOVED %d 127.0.0.1:%d" % (moved, self.b.addr[1]))
                                       if args[1] == k1 else "OK")
        self.assertEqual(self.client.pipeline().call("SET", k1, "1").call("SET", k2, "2").execute(), ["OK", "OK"])
        self.assertEqual(self.b.commands("SET"), [("SET", k1, "1")])
        self.assertEqual(len(self.a.commands("SET")), 2)  # k2 wasn't resent
        self.assertEqual(self.client.slots[moved], self.b.addr)

    def test_ask_inside_a_batch_is_sent_as_asking_pairs(self):
        k1, k2 = self.keys_on(self.a, 2)
        self.a.handler = lambda args: (ReplyError("ASK %d 127.0.0.1:%d" % (key_slot(args[1]), self.b.addr[1]))
                                       if args[1] in (k1, k2) else "OK")
        self.client.pipeline().call("GET", k1).call("GET", k2).execute()
        self.assertEqual(self.b.log, [("ASKING",), ("GET", k1), ("ASKING",), ("GET", k2)])

    def test_node_dying_mid_batch(self):
        k = self.keys_on(self.a, 4)
        kb = self.keys_on(self.b, 1)[0]
        dies_once = [True]

        def handler(args):
            if args == ["GET", k[1]] and dies_once:
                dies_once.pop()
                return CLOSE  # the SET before it ran; nothing after it gets a reply
            return b"v" if args[0] == "GET" else "OK"
        self.a.handler = handler
        self.client.retry_timeout = 10  # a refused connect takes ~2s on Windows
        results = (self.client.pipeline().call("SET", k[0], "0").call("GET", k[1]).call("SET", k[2], "2")
                   .call("GET", k[3]).call("SET", kb, "b").execute(raise_on_error=False))
        self.assertEqual(results[0], "OK")  # answered before the connection died: final
        self.assertEqual(results[1], b"v")  # a lost read: retried
        self.assertIsInstance(results[2], UncertainWriteError)  # a lost write: not retried
        self.assertEqual(results[3], b"v")  # a lost read, in another slot: retried
        self.assertEqual(results[4], "OK")  # the other node was unaffected
        # The SET on k[2] was sent but never reached the handler (the node
        # died first), and the client didn't send it again.
        self.assertEqual(self.a.commands("SET"), [("SET", k[0], "0")])

    def test_lost_read_before_lost_write_to_same_slot_is_not_retried(self):
        r, w = self.keys_on(self.a, 2, tag="t")  # same slot
        self.a.handler = lambda args: CLOSE
        results = self.client.pipeline().call("GET", r).call("SET", w, "1").execute(raise_on_error=False)
        # Retrying the GET could let it see the SET queued after it.
        self.assertIsInstance(results[0], ConnectionError)
        self.assertNotIsInstance(results[0], UncertainWriteError)
        self.assertIsInstance(results[1], UncertainWriteError)
        self.assertEqual(len(self.a.commands("GET")), 1)

    def test_lost_read_after_lost_write_to_same_slot_is_retried(self):
        w, r = self.keys_on(self.a, 2, tag="t")
        dies_once = [True]

        def handler(args):
            if dies_once:
                dies_once.pop()
                return CLOSE
            return b"v"
        self.a.handler = handler
        results = self.client.pipeline().call("SET", w, "1").call("GET", r).execute(raise_on_error=False)
        self.assertIsInstance(results[0], UncertainWriteError)
        self.assertEqual(results[1], b"v")  # either outcome of the SET is a valid thing to see

    def test_command_error_raises_after_everything_ran(self):
        k1, k2 = self.keys_on(self.a, 2)
        self.a.handler = lambda args: ReplyError("ERR boom") if args[1] == k1 else "OK"
        pipe = self.client.pipeline().call("SET", k1, "1").call("SET", k2, "2")
        with self.assertRaisesRegex(ReplyError, "boom"):
            pipe.execute()
        self.assertEqual(len(self.a.commands("SET")), 2)  # the second command still ran
        results = self.client.pipeline().call("SET", k1, "1").call("SET", k2, "2").execute(raise_on_error=False)
        self.assertIsInstance(results[0], ReplyError)
        self.assertEqual(results[1], "OK")

    def test_chunks_keep_order(self):
        keys = self.keys_on(self.a, 7)
        pipe = self.client.pipeline(chunk=3)
        for key in keys:
            pipe.call("GET", key)
        self.assertEqual(pipe.execute(), [b"v:" + key.encode() for key in keys])
        self.assertEqual(self.a.commands("GET"), [("GET", key) for key in keys])

    def test_cross_slot_rejected_when_queued(self):
        ka, kb = self.keys_on(self.a, 1)[0], self.keys_on(self.b, 1)[0]
        with self.assertRaises(CrossSlotError):
            self.client.pipeline().call("DEL", ka, kb)


if __name__ == "__main__":
    unittest.main()
