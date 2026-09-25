"""Log-linear latency histogram (the idea behind HdrHistogram).

Storing every latency sample costs memory proportional to the request count
(millions per run). A histogram with fixed *relative* precision costs a few
hundred buckets whatever the count, and merges across processes by adding
counts. Buckets double in width every power of two, and each power of two is
split into 2**SUB_BITS equal sub-buckets, so any value is known to within
about 1/2**SUB_BITS of itself: 3% at SUB_BITS = 5, whether it is 20us or 20ms.
"""

SUB_BITS = 5
_TOP_BITS = SUB_BITS + 1  # the leading 1 plus SUB_BITS below it


def bucket(value):
    """Monotonic bucket key: larger values never get smaller keys."""
    if value < (1 << _TOP_BITS):
        return value  # small values are exact
    shift = value.bit_length() - _TOP_BITS
    return (shift << _TOP_BITS) + (value >> shift)


def bucket_upper(key):
    """Largest value that lands in bucket `key`."""
    shift, top = key >> _TOP_BITS, key & ((1 << _TOP_BITS) - 1)
    if shift == 0:
        return top
    return ((top + 1) << shift) - 1


class Histogram:
    def __init__(self, counts=None):
        self.counts = dict(counts or {})
        self.max = 0
        self.total = 0
        self.sum = 0

    def record(self, value):
        k = bucket(value)
        self.counts[k] = self.counts.get(k, 0) + 1
        self.total += 1
        self.sum += value
        if value > self.max:
            self.max = value

    def merge(self, other):
        for k, n in other.counts.items():
            self.counts[k] = self.counts.get(k, 0) + n
        self.total += other.total
        self.sum += other.sum
        self.max = max(self.max, other.max)

    def percentile(self, p):
        """Smallest bucket bound with at least p% of samples at or below it.
        Reports the bucket's upper bound, so it can overstate a percentile
        by up to one bucket width but never understate it."""
        if self.total == 0:
            return 0
        rank = p / 100.0 * self.total
        seen = 0
        for k in sorted(self.counts):
            seen += self.counts[k]
            if seen >= rank:
                return min(bucket_upper(k), self.max)
        return self.max

    def mean(self):
        return self.sum / self.total if self.total else 0

    def state(self):
        """Picklable form, to send from a worker process to the parent."""
        return {"counts": self.counts, "max": self.max, "total": self.total, "sum": self.sum}

    @classmethod
    def from_state(cls, s):
        h = cls(s["counts"])
        h.max, h.total, h.sum = s["max"], s["total"], s["sum"]
        return h
