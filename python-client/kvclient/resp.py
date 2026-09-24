"""RESP wire format and a blocking connection to one server."""

import socket


class ReplyError(Exception):
    """An error reply (-ERR ..., -MOVED ...) from the server."""


def encode(*args):
    """One command as a RESP array of bulk strings."""
    out = [b"*%d\r\n" % len(args)]
    for a in args:
        a = a if isinstance(a, bytes) else str(a).encode()
        out.append(b"$%d\r\n%s\r\n" % (len(a), a))
    return b"".join(out)


class Conn:
    """Minimal blocking RESP connection. call() raises error replies;
    call_raw() returns them as ReplyError objects."""

    def __init__(self, host, port, timeout=10):
        self.host, self.port = host, int(port)
        self.timeout = timeout
        self.sock = socket.create_connection((self.host, self.port), timeout=timeout)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.buf = b""

    def close(self):
        self.sock.close()

    def peer_closed(self):
        """True if the server closed this connection while it sat idle.

        A non-blocking peek: an idle connection has nothing to read, so
        EOF (b"") or a reset means the peer is gone. Checked before sending,
        so a dead cached connection is replaced before a command could be
        half-delivered."""
        if self.buf:
            return False
        self.sock.setblocking(False)
        try:
            return self.sock.recv(1, socket.MSG_PEEK) == b""
        except (BlockingIOError, InterruptedError):
            return False
        except OSError:
            return True
        finally:
            self.sock.settimeout(self.timeout)

    def _fill(self):
        chunk = self.sock.recv(65536)
        if not chunk:
            raise ConnectionError("connection closed by %s:%d" % (self.host, self.port))
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
            return ReplyError(rest.decode())
        if kind == b":":
            return int(rest)
        if kind == b"$":
            n = int(rest)
            if n < 0:
                return None
            while len(self.buf) < n + 2:
                self._fill()
            data, self.buf = self.buf[:n], self.buf[n + 2:]
            return data
        if kind == b"*":
            n = int(rest)
            return None if n < 0 else [self.read_reply() for _ in range(n)]
        raise ValueError("bad reply line %r" % line)

    def send(self, *args):
        self.sock.sendall(encode(*args))

    def send_many(self, commands):
        """Several commands in one write; read their replies in order."""
        self.sock.sendall(b"".join(encode(*c) for c in commands))

    def call_raw(self, *args):
        """Returns error replies as ReplyError objects instead of raising."""
        self.send(*args)
        return self.read_reply()

    def call(self, *args):
        r = self.call_raw(*args)
        if isinstance(r, ReplyError):
            raise r
        return r
