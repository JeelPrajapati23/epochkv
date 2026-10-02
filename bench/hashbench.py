#!/usr/bin/env python3
"""HashTable microbenchmark: the working tree's hash_table.hpp vs. a baseline.

  hashbench.py --baseline REV [--sizes N,N,...] [--rounds R] [--lookups L] [--cpu C]

Compiles bench/hashbench.cpp twice, against include/hash_table.hpp and
against REV's version of it (from git), with the Release build's flags.
Then runs the two binaries alternately, R rounds per table size, pinned to
one CPU, and reports the median ns per find (random order) and per insert
(including the resizes the inserts trigger).

For changes too small to see through the server: a GET spends most of its
time in the kernel, so saving a few ns per lookup is lost in bench.py's
run-to-run noise. Numbers from here overstate the server-level effect: the
loop does nothing but independent lookups.

Linux only (taskset). Needs g++.
"""

import argparse
import os
import statistics
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from bench import ROOT, git_commit, machine_info, save  # noqa: E402

SOURCE = os.path.join(HERE, "hashbench.cpp")
HEADER = os.path.join(ROOT, "include", "hash_table.hpp")
CXXFLAGS = ["-O3", "-DNDEBUG", "-std=c++17"]  # CMake's Release flags


def short_rev(rev):
    return subprocess.run(["git", "-C", ROOT, "rev-parse", "--short", rev],
                          capture_output=True, text=True, check=True).stdout.strip()


def build(workdir, name, header_text):
    include_dir = os.path.join(workdir, name)
    os.makedirs(include_dir)
    with open(os.path.join(include_dir, "hash_table.hpp"), "w") as f:
        f.write(header_text)
    binary = os.path.join(workdir, "hashbench-" + name)
    subprocess.run(["g++", *CXXFLAGS, "-I", include_dir, SOURCE, "-o", binary], check=True)
    return binary


def run_once(binary, keys, lookups, cpu):
    out = subprocess.run(["taskset", "-c", str(cpu), binary, str(keys), str(lookups)],
                         capture_output=True, text=True, check=True).stdout.split()
    return {"find_ns": float(out[1]), "insert_ns": float(out[2]), "checksum": int(out[3])}


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--baseline", required=True, help="git revision whose hash_table.hpp to compare against")
    p.add_argument("--sizes", default="1000,100000,1000000,5000000")
    p.add_argument("--rounds", type=int, default=5)
    p.add_argument("--lookups", type=int, default=20_000_000)
    p.add_argument("--cpu", type=int, default=2)
    args = p.parse_args()
    sizes = [int(s) for s in args.sizes.split(",")]

    baseline_header = subprocess.run(["git", "-C", ROOT, "show", "%s:include/hash_table.hpp" % args.baseline],
                                     capture_output=True, text=True, check=True).stdout
    with open(HEADER) as f:
        current_header = f.read()

    with tempfile.TemporaryDirectory() as workdir:
        binaries = {"baseline": build(workdir, "baseline", baseline_header),
                    "current": build(workdir, "current", current_header)}

        rows = []
        print("%10s  %-8s  %12s  %14s" % ("keys", "version", "find ns", "insert ns"))
        for keys in sizes:
            runs = {"baseline": [], "current": []}
            # Alternate, so drift (thermals, other load) hits both equally.
            for _ in range(args.rounds):
                for version, binary in binaries.items():
                    runs[version].append(run_once(binary, keys, args.lookups, args.cpu))
            checksums = {r["checksum"] for rs in runs.values() for r in rs}
            if len(checksums) != 1:
                sys.exit("versions disagree on lookup results at %d keys: %s" % (keys, checksums))

            row = {"keys": keys}
            for version, rs in runs.items():
                row[version] = {
                    "find_ns_median": statistics.median(r["find_ns"] for r in rs),
                    "insert_ns_median": statistics.median(r["insert_ns"] for r in rs),
                    "runs": rs,
                }
                print("%10d  %-8s  %12.1f  %14.1f" % (keys, version, row[version]["find_ns_median"],
                                                      row[version]["insert_ns_median"]))
            for metric in ("find", "insert"):
                base = row["baseline"]["%s_ns_median" % metric]
                cur = row["current"]["%s_ns_median" % metric]
                row["%s_change_pct" % metric] = (cur - base) / base * 100
            print("%10s  %-8s  %+11.1f%%  %+13.1f%%" % ("", "change", row["find_change_pct"],
                                                       row["insert_change_pct"]))
            rows.append(row)

    doc = {"meta": {"server": "kv", "commit": git_commit(), "baseline": short_rev(args.baseline),
                    "date": time.strftime("%Y-%m-%d %H:%M:%S"), "machine": machine_info(),
                    "cxxflags": CXXFLAGS, "lookups": args.lookups, "rounds": args.rounds, "cpu": args.cpu},
           "rows": rows}
    save("hashbench", doc)


if __name__ == "__main__":
    main()
