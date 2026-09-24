"""Cluster-aware client: routes each command to the master that owns its
keys' hash slot, and follows the cluster's redirects."""

import time

from .resp import Conn, ReplyError

SLOTS = 16384


def _crc16_table():
    # CRC of every possible high byte, from the bitwise definition
    # (CRC16/XMODEM, polynomial 0x1021): one lookup per key byte afterwards.
    table = []
    for i in range(256):
        crc = i << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) if crc & 0x8000 else crc << 1
        table.append(crc & 0xFFFF)
    return table


_CRC16 = _crc16_table()


def crc16(data):
    crc = 0
    for byte in data:
        crc = ((crc << 8) & 0xFFFF) ^ _CRC16[(crc >> 8) ^ byte]
    return crc


def key_slot(key):
    """Hash slot of a key, honoring {hash tags}: only the part between the
    first '{' and the next '}' is hashed, if that part is non-empty."""
    key = key if isinstance(key, bytes) else str(key).encode()
    start = key.find(b"{")
    if start != -1:
        end = key.find(b"}", start + 1)
        if end > start + 1:
            key = key[start + 1:end]
    return crc16(key) & (SLOTS - 1)


# Where each command's keys are, mirroring the server's command table:
# (first, last, step, is_write). last == -1 means "through the last
# argument". Commands missing from here are routed by an explicit key=
# argument, or sent to any node, and are treated as writes when retrying.
_KEY_SPECS = {
    "GET": (1, 1, 1, False),
    "TTL": (1, 1, 1, False),
    "PTTL": (1, 1, 1, False),
    "DUMP": (1, 1, 1, False),
    "EXISTS": (1, -1, 1, False),
    "SET": (1, 1, 1, True),
    "DEL": (1, -1, 1, True),
    "EXPIRE": (1, 1, 1, True),
    "PEXPIRE": (1, 1, 1, True),
    "EXPIREAT": (1, 1, 1, True),
    "PEXPIREAT": (1, 1, 1, True),
    "PERSIST": (1, 1, 1, True),
    "RESTORE": (1, 1, 1, True),
}
_KEYLESS_READS = {"PING", "ECHO", "DBSIZE", "INFO", "CLUSTER"}


def command_keys(args):
    """(keys, is_write) for one command."""
    name = (args[0].decode() if isinstance(args[0], bytes) else str(args[0])).upper()
    spec = _KEY_SPECS.get(name)
    if spec is None:
        return [], name not in _KEYLESS_READS
    first, last, step, is_write = spec
    last = len(args) - 1 if last == -1 else last
    return list(args[first:last + 1:step]), is_write


class CrossSlotError(ReplyError):
    """A multi-key command whose keys hash to different slots."""


class UncertainWriteError(ConnectionError):
    """The connection died after a write was sent and before its reply:
    the write may or may not have been applied, so it isn't retried."""


def parse_addr(s):
    host, port = s.rsplit(":", 1)
    return host, int(port)


class ClusterClient:
    """Routes each command to the master owning its keys' slot.

    Follows MOVED (update that slot in the map) and ASK (send ASKING plus
    this one command to the target, map unchanged), and rides out failovers:
    on CLUSTERDOWN or a dead node it reloads the map from any node that
    answers and retries, for up to `retry_timeout` seconds. A write whose
    connection dies after it was sent raises UncertainWriteError instead of
    being retried (at-most-once)."""

    def __init__(self, seed, max_redirects=16, retry_timeout=30, timeout=5):
        self.seed = tuple(seed)
        self.known = [self.seed]  # healthiest first: failing nodes move to the back
        self.max_redirects = max_redirects
        self.retry_timeout = retry_timeout
        self.timeout = timeout
        self.conns = {}
        self.slots = [None] * SLOTS  # slot -> (host, port) of its master
        self.redirects = {"MOVED": 0, "ASK": 0, "TRYAGAIN": 0}
        self.retries = {"connection": 0, "CLUSTERDOWN": 0}
        self.refresh()

    # -- connections -------------------------------------------------------

    def _learn(self, addr):
        if addr not in self.known:
            self.known.append(addr)

    def conn(self, addr):
        """A live connection to addr. Connect errors raise OSError, and are
        always safe to retry: nothing was sent."""
        c = self.conns.get(addr)
        if c is not None and c.peer_closed():
            self.drop(addr)
            c = None
        if c is None:
            c = self.conns[addr] = Conn(*addr, timeout=self.timeout)
        return c

    def drop(self, addr):
        """Closes addr's connection and moves it to the back of the list."""
        c = self.conns.pop(addr, None)
        if c is not None:
            c.close()
        if addr in self.known and len(self.known) > 1:
            self.known.remove(addr)
            self.known.append(addr)

    def close(self):
        for c in self.conns.values():
            c.close()
        self.conns = {}

    # -- slot map ----------------------------------------------------------

    def refresh(self):
        """Reloads the slot map from the first known node that answers."""
        for addr in list(self.known):
            try:
                ranges = self.conn(addr).call("CLUSTER", "SLOTS")
            except (OSError, ReplyError):
                self.drop(addr)
                continue
            slots = [None] * SLOTS
            for start, end, *nodes in ranges:
                for node in nodes:
                    self._learn((node[0].decode(), node[1]))
                master = (nodes[0][0].decode(), nodes[0][1])
                slots[start:end + 1] = [master] * (end - start + 1)
            self.slots = slots
            return
        raise ConnectionError("no known cluster node answers")

    def keyslot(self, key):
        return key_slot(key)

    def _owner(self, slot):
        # Unknown or keyless: any node will do. A wrong guess costs one MOVED.
        owner = self.slots[slot] if slot is not None else None
        return owner or self.known[0]

    def _recover(self, deadline, reason):
        """Waits a moment for the cluster to reconfigure, then reloads the
        map. Raises once the retry budget is spent."""
        self.retries[reason] += 1
        if time.monotonic() > deadline:
            return False
        time.sleep(0.1)
        try:
            self.refresh()
        except ConnectionError:
            pass
        return True

    # -- commands ----------------------------------------------------------

    def call(self, *args, key=None):
        keys, is_write = command_keys(args)
        if key is not None:
            keys = [key]
        slots = {key_slot(k) for k in keys}
        if len(slots) > 1:
            raise CrossSlotError("CROSSSLOT Keys in request don't hash to the same slot")
        slot = slots.pop() if slots else None

        addr, asking, redirects = self._owner(slot), False, 0
        deadline = time.monotonic() + self.retry_timeout
        while True:
            try:
                c = self.conn(addr)
            except OSError:
                # Nothing was sent: safe to retry, reads and writes alike.
                self.drop(addr)
                if not self._recover(deadline, "connection"):
                    raise
                addr, asking = self._owner(slot), False
                continue
            try:
                if asking:
                    c.send_many([("ASKING",), args])  # one round trip, not two
                    c.read_reply()
                else:
                    c.send(*args)
                r = c.read_reply()
            except OSError:
                # Sent, but no reply: the command may or may not have run.
                self.drop(addr)
                if is_write:
                    raise UncertainWriteError(
                        "connection to %s:%d lost after sending %r; it may or may not have been applied"
                        % (addr[0], addr[1], args[0]))
                if not self._recover(deadline, "connection"):
                    raise
                addr, asking = self._owner(slot), False
                continue

            if not isinstance(r, ReplyError):
                return r
            # Every error below means the server did NOT run the command,
            # so retrying is safe even for writes.
            kind, _, rest = str(r).partition(" ")
            if kind in ("MOVED", "ASK"):
                redirects += 1
                if redirects > self.max_redirects:
                    raise ReplyError("too many redirects for %r" % (args,))
                self.redirects[kind] += 1
                moved_slot, target = rest.split()
                target = parse_addr(target)
                self._learn(target)
                if kind == "MOVED":
                    self.slots[int(moved_slot)] = target  # ownership changed for good
                addr, asking = target, kind == "ASK"  # ASK: this command only
            elif kind == "TRYAGAIN" and time.monotonic() < deadline:
                # Keys split between source and target mid-migration.
                self.redirects[kind] += 1
                time.sleep(0.01)
                addr, asking = self._owner(slot), False
            elif kind == "CLUSTERDOWN" and self._recover(deadline, "CLUSTERDOWN"):
                addr, asking = self._owner(slot), False
            else:
                raise r
