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


class _Cmd:
    """A queued command and where it's headed next."""

    __slots__ = ("args", "slot", "is_write", "addr", "asking", "redirects", "error")

    def __init__(self, args, slot, is_write):
        self.args, self.slot, self.is_write = args, slot, is_write
        self.addr, self.asking, self.redirects, self.error = None, False, 0, None


_PENDING = object()


class Pipeline:
    """Queues commands and sends them in batches: one write per node, all
    nodes at once, then the replies are read and put back in queue order.

    Not a transaction: other clients' commands can run in between, and a
    failed command doesn't undo the ones before it. Commands on the same key
    run in queue order; commands on different keys may run in any order."""

    def __init__(self, client, chunk=10000):
        self.client = client
        self.chunk = chunk  # bounds the replies a node buffers for us
        self.queue = []

    def __len__(self):
        return len(self.queue)

    def call(self, *args, key=None):
        keys, is_write = command_keys(args)
        if key is not None:
            keys = [key]
        slots = {key_slot(k) for k in keys}
        if len(slots) > 1:
            raise CrossSlotError("CROSSSLOT Keys in request don't hash to the same slot")
        self.queue.append(_Cmd(args, slots.pop() if slots else None, is_write))
        return self

    def execute(self, raise_on_error=True):
        """One result per queued command, in order. A failed command's slot
        holds its exception. With raise_on_error, the first one is raised,
        but only after every command has its result."""
        cmds, self.queue = self.queue, []
        results = []
        for start in range(0, len(cmds), self.chunk):
            results += self.client._execute(cmds[start:start + self.chunk])
        if raise_on_error:
            for r in results:
                if isinstance(r, Exception):
                    raise r
        return results


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
            except ReplyError as e:
                if "cluster support disabled" in str(e):
                    self.slots = [addr] * SLOTS  # a standalone server owns every key
                    return
                self.drop(addr)
                continue
            except OSError:
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

    def _recover(self, deadline, reason=None):
        """Waits a moment for the cluster to reconfigure, then reloads the
        map. False once the retry budget is spent."""
        if reason:
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

    def pipeline(self, chunk=10000):
        return Pipeline(self, chunk)

    def call(self, *args, key=None):
        """One command: a pipeline of one, so there's a single copy of the
        routing and retry rules."""
        return self.pipeline().call(*args, key=key).execute()[0]

    def _execute(self, cmds):
        """Runs a batch until every command has a final result. Returns the
        results in order, errors as exception objects."""
        results = [_PENDING] * len(cmds)
        for c in cmds:
            c.addr = self._owner(c.slot)
        pending = list(range(len(cmds)))
        deadline = time.monotonic() + self.retry_timeout
        while pending:
            groups = {}
            for i in pending:
                cmds[i].error = None
                groups.setdefault(cmds[i].addr, []).append(i)
            retry, reasons = [], set()

            # 1. Every node gets its commands before any reply is read, so
            #    the nodes work in parallel.
            sent = []
            for addr, idxs in groups.items():
                try:
                    conn = self.conn(addr)
                except OSError as e:
                    self.drop(addr)  # nothing was sent: safe to retry them all
                    for i in idxs:
                        cmds[i].error = e
                    retry += idxs
                    reasons.add("connection")
                    continue
                frames = []
                for i in idxs:
                    if cmds[i].asking:
                        frames.append(("ASKING",))
                    frames.append(cmds[i].args)
                try:
                    conn.send_many(frames)
                except OSError as e:
                    self.drop(addr)
                    retry += self._lost(cmds, idxs, results, addr, e)
                    reasons.add("connection")
                    continue
                sent.append((addr, conn, idxs))

            # 2. Each node's replies come back in the order it got the commands.
            for addr, conn, idxs in sent:
                for n, i in enumerate(idxs):
                    try:
                        if cmds[i].asking:
                            conn.read_reply()  # ASKING's +OK
                        r = conn.read_reply()
                    except OSError as e:
                        self.drop(addr)
                        retry += self._lost(cmds, idxs[n:], results, addr, e)
                        reasons.add("connection")
                        break
                    if self._handle(cmds[i], r, results, i):
                        retry.append(i)
                        if cmds[i].error is not None:
                            reasons.add(str(r).split(" ", 1)[0])

            # 3. Errors that need the cluster to settle (a dead node,
            #    CLUSTERDOWN, TRYAGAIN) share one wait and one map reload.
            waiting = [i for i in retry if cmds[i].error is not None]
            if waiting:
                reason = "connection" if "connection" in reasons else (
                    "CLUSTERDOWN" if "CLUSTERDOWN" in reasons else None)
                if self._recover(deadline, reason):
                    for i in waiting:
                        cmds[i].addr, cmds[i].asking = self._owner(cmds[i].slot), False
                else:
                    for i in waiting:
                        results[i] = cmds[i].error  # budget spent: the last error is final
                    retry = [i for i in retry if results[i] is _PENDING]
            # Queue order again, so commands on the same key keep their order.
            pending = sorted(retry)
        return results

    def _handle(self, cmd, r, results, i):
        """Records reply r for command i. True if it must be retried."""
        if not isinstance(r, ReplyError):
            results[i] = r
            return False
        # Every error handled below means the server did NOT run the
        # command, so retrying is safe even for writes.
        kind, _, rest = str(r).partition(" ")
        if kind in ("MOVED", "ASK"):
            cmd.redirects += 1
            if cmd.redirects > self.max_redirects:
                results[i] = ReplyError("too many redirects for %r" % (cmd.args,))
                return False
            self.redirects[kind] += 1
            moved_slot, target = rest.split()
            target = parse_addr(target)
            self._learn(target)
            if kind == "MOVED":
                self.slots[int(moved_slot)] = target  # ownership changed for good
            cmd.addr, cmd.asking = target, kind == "ASK"  # ASK: this command only
            return True
        if kind in ("TRYAGAIN", "CLUSTERDOWN"):
            if kind == "TRYAGAIN":
                self.redirects[kind] += 1  # keys split between source and target mid-migration
            cmd.error = r
            return True
        results[i] = r  # a real command error (wrong type, bad arguments...)
        return False

    def _lost(self, cmds, idxs, results, addr, exc):
        """Commands sent to addr whose replies never came: each may or may
        not have run. Writes fail with UncertainWriteError. Reads are
        retried, unless a lost write to the same slot comes after one in
        the batch: retried now, the read could see that later write, and
        per-key order would break. Returns the indices to retry."""
        retry, uncertain_slots = [], set()
        for i in reversed(idxs):
            c = cmds[i]
            if c.is_write:
                results[i] = UncertainWriteError(
                    "connection to %s:%d lost after sending %r; it may or may not have been applied"
                    % (addr[0], addr[1], c.args[0]))
                uncertain_slots.add(c.slot)
            elif c.slot in uncertain_slots:
                results[i] = ConnectionError(
                    "connection to %s:%d lost; %r not retried: a later write to the same slot "
                    "in this batch may or may not have been applied" % (addr[0], addr[1], c.args[0]))
            else:
                c.error = exc
                retry.append(i)
        return retry
