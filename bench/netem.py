#!/usr/bin/env python3
"""Network-emulation experiments on the Docker setup (bench/docker/).

  netem.py latency [--rtt 0 0.5 1 2 5]
  netem.py failover [--profiles none cross-az lossy bad] [--mode kill|pause]

Each node and the client are separate containers with their own IPs.
Network conditions come from tc netem on each container's outgoing traffic
(eth0): a one-way delay of d on both ends makes a round trip of 2d.

latency: one standalone server, and the closed-loop benchmark from the client
  container at each RTT, without pipelining and 16 deep. Shows how latency
  caps throughput (one client can't do more than 1/RTT requests a second)
  and how pipelining gets it back.

failover: a fresh 3-master + 3-replica cluster per network profile, with a
  writer running. First a quiet period, counting false suspicions (a healthy
  node flagged fail?) caused only by the network. Then a master is either
  killed (kill -9: a crash) or frozen (docker pause, like a long GC pause or
  a stalled VM: its kernel still accepts connections, the process doesn't
  run) and later thawed. Reports how long detection, agreement, promotion
  and recovery took, how many acknowledged writes were lost, and, for a
  freeze, whether the old master accepted writes after waking up.

Runs on the host (WSL); the measuring happens in the client container
(bench/netem_client.py), which can reach the nodes' container IPs. Starts
the containers if needed. Results go to bench/results/.
"""

import argparse
import json
import os
import subprocess
import sys
import time

import bench

HERE = os.path.dirname(os.path.abspath(__file__))
COMPOSE = ["docker", "compose", "-f", os.path.join(HERE, "docker", "compose.yml")]
NODES = ["node%d" % i for i in range(1, 7)]
PORT = 6379
IN_CONTAINER = "/opt/epochkv"

# One-way delay (ms), jitter (ms), loss (%) on every node's outgoing traffic.
# Node-to-node round trips are twice the delay.
PROFILES = {
    "none": (0, 0, 0),
    "same-dc": (0.25, 0.05, 0),  # ~0.5ms RTT
    "cross-az": (1, 0.2, 0),     # ~2ms RTT, like availability zones in one region
    "lossy": (1, 0.2, 1),        # the same, with 1% packet loss
    "bad": (2.5, 1, 5),          # ~5ms RTT, heavy jitter, 5% loss
}


def dc(*args, check=True):
    return subprocess.run(COMPOSE + list(args), capture_output=True, text=True, check=check).stdout


def run_in(service, *cmd, check=True):
    return dc("exec", "-T", service, *cmd, check=check)


def ip(service):
    return run_in(service, "hostname", "-i").split()[0]


def shape(service, delay_ms=0, jitter_ms=0, loss_pct=0):
    """Sets (or clears) netem on a container's outgoing traffic."""
    run_in(service, "tc", "qdisc", "del", "dev", "eth0", "root", check=False)
    if delay_ms or loss_pct:
        cmd = ["tc", "qdisc", "add", "dev", "eth0", "root", "netem", "delay", "%gms" % delay_ms]
        if jitter_ms:
            cmd.append("%gms" % jitter_ms)
        if loss_pct:
            cmd += ["loss", "%g%%" % loss_pct]
        run_in(service, *cmd)


def wait_reachable(addr, timeout=30):
    probe = ("import socket,time,sys\n"
             "d=time.time()+%d\n"
             "while True:\n"
             "    try: socket.create_connection((%r,%d),timeout=1).close(); break\n"
             "    except OSError:\n"
             "        if time.time()>d: sys.exit(1)\n"
             "        time.sleep(0.1)\n") % (timeout, addr[0], addr[1])
    run_in("client", "python3", "-c", probe)


def bench_in_client(*args):
    """Runs bench.py in the client container; returns its saved JSON run."""
    name = "netem-tmp"
    run_in("client", "python3", "bench/bench.py", "run", *args, "--save", name)
    out = run_in("client", "sh", "-c", "cat bench/results/%s-*.json && rm bench/results/%s-*.json" % (name, name))
    return json.loads(out)["runs"][0]


def save(name, doc):
    doc["meta"] = {"server": "kv", "commit": bench.git_commit(), "date": time.strftime("%Y-%m-%d %H:%M:%S"),
                   "machine": bench.machine_info(), **doc.get("meta", {})}
    bench.save(name, doc)


# -- latency ---------------------------------------------------------------------

def cmd_latency(args):
    dc("up", "-d", "--build")
    # A standalone server next to node1's cluster node (which stays idle).
    # [p]: the pattern then doesn't match this sh's own command line.
    run_in("node1", "sh", "-c", "pkill -f '[p]ort 7000' ; true", check=False)
    dc("exec", "-d", "node1", "kv_server", "--bind", "0.0.0.0", "--port", "7000", "--save", "", "--dir", "/tmp")
    target = (ip("node1"), 7000)
    wait_reachable(target)
    rows = []
    try:
        for rtt in args.rtt:
            for service in ("node1", "client"):
                shape(service, rtt / 2)
            row = {"rtt_ms": rtt}
            for label, clients, pipeline in (("1 client", 1, 1), ("16 clients", 16, 1), ("16 clients x16", 16, 16)):
                r = bench_in_client("--target", "%s:%d" % target, "--clients", str(clients),
                                    "--pipeline", str(pipeline), "--duration", str(args.duration), "--warmup", "1")
                row[label] = {"ops_per_sec": r["ops_per_sec"], "p50_us": r["latency_us"]["p50"],
                              "p99_us": r["latency_us"]["p99"]}
            rows.append(row)
            print("RTT %4.1fms   1 client %7s ops/s (p50 %6.0fus)   16 clients %8s ops/s   "
                  "16 clients x16 %9s ops/s (p50 %6.0fus)" % (
                      rtt, "{:,}".format(row["1 client"]["ops_per_sec"]), row["1 client"]["p50_us"],
                      "{:,}".format(row["16 clients"]["ops_per_sec"]),
                      "{:,}".format(row["16 clients x16"]["ops_per_sec"]), row["16 clients x16"]["p50_us"]),
                  flush=True)
    finally:
        for service in ("node1", "client"):
            shape(service)
    save("netem-latency", {"meta": {"setup": "docker: server and client containers, netem on both"},
                           "runs": rows})


# -- failover --------------------------------------------------------------------

def fresh_cluster():
    """Recreated containers (empty data, new node ids), joined into a cluster.
    node1-3 are masters; node4-6 replicate them in order."""
    dc("up", "-d", "--build", "--force-recreate", *NODES)
    addrs = [(ip(n), PORT) for n in NODES]
    for a in addrs:
        wait_reachable(a)
    run_in("client", "python3", "tools/kv_cluster.py", "create",
           *["%s:%d" % a for a in addrs], "--replicas", "1")
    return addrs


def first(events, t_from, **match):
    for e in events:
        if e["t"] >= t_from and all(e.get(k) == v for k, v in match.items()):
            return e["t"]
    return None


def analyse(result, mode, victim_addr, t_event, t_thaw):
    events = result["events"]
    by_addr = {n["addr"]: n for n in result["topology"]}
    victim = by_addr[victim_addr]["id"]
    replica = next(n["id"] for n in result["topology"] if n["master"] == victim)
    quiet = [e for e in events if e["t"] < t_event and e["event"] in ("pfail", "fail") and e["value"]]
    since = lambda t: round(t - t_event, 2) if t is not None else None  # noqa: E731

    # Recovered: every observer that saw cluster_state go to fail has seen it
    # come back to ok.
    recovered = None
    for obs in {e["observer"] for e in events if e["event"] == "state" and e["t"] >= t_event}:
        if obs == victim_addr:
            continue
        down = first(events, t_event, observer=obs, event="state", value="fail")
        if down is not None:
            up = first(events, down, observer=obs, event="state", value="ok")
            recovered = None if up is None else max(recovered or up, up)
    out = {
        "quiet_seconds": round(t_event - result["started"], 1),
        "false_pfail_events": len([e for e in quiet if e["event"] == "pfail"]),
        "false_fail_events": len([e for e in quiet if e["event"] == "fail"]),
        # PFAIL can turn into FAIL between two polls, so whichever is seen first.
        "detected_s": since(min((t for t in (first(events, t_event, event=e, value=True, subject_id=victim)
                                             for e in ("pfail", "fail")) if t is not None), default=None)),
        "agreed_fail_s": since(first(events, t_event, event="fail", value=True, subject_id=victim)),
        "replica_promoted_s": since(first(events, t_event, event="role", value="master", subject_id=replica)),
        "cluster_ok_again_s": since(recovered),
        "writer": result["writer"],
    }
    if mode == "pause":
        stale = result["stale"]
        # Neither docker command's timestamp marks the real freeze or wake:
        # `pause` takes effect some time after it's issued (OKs still come
        # back), and `unpause` returns some time after the process runs
        # again. The stale writer's first timeout is certain to fall inside
        # the freeze, so every reply after it came after the wake.
        frozen = next((r[0] for r in stale["replies"] if r[0] >= t_event and r[1] == "timeout"), None)
        after = [r for r in stale["replies"] if frozen is not None and r[0] > frozen]
        ok_after = [r for r in after if r[1] == "ok"]
        out["thawed_at_s"] = since(t_thaw)
        out["victim_demoted_s"] = since(first(events, t_thaw, event="role", value="replica", subject_id=victim))
        out["stale_writes_accepted_after_thaw"] = len(ok_after)
        out["stale_first_reply_after_thaw"] = next((r[1] for r in after if r[1] != "timeout"), None)
        out["stale_accepted_write_survived"] = (stale["final_value"] is not None and
                                                any(str(r[2]) == stale["final_value"] for r in ok_after))
    return out


def cmd_failover(args):
    dc("up", "-d", "--build")
    runs = []
    for profile in args.profiles:
        addrs = fresh_cluster()
        for n in NODES:
            shape(n, *PROFILES[profile])
        victim_service = "node2"
        victim_addr = "%s:%d" % (ip(victim_service), PORT)
        seconds = args.quiet + args.pause + args.after
        cmd = COMPOSE + ["exec", "-T", "client", "python3", "bench/netem_client.py", "watch",
                         "--nodes", ",".join("%s:%d" % a for a in addrs), "--seconds", str(seconds)]
        if args.mode == "pause":
            cmd += ["--stale-target", victim_addr]
        watcher = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True)
        time.sleep(args.quiet + 1)  # the watcher needs a moment to start
        t_event, t_thaw = time.time(), None
        if args.mode == "kill":
            dc("kill", "-s", "KILL", victim_service)
        else:
            dc("pause", victim_service)
            time.sleep(args.pause)
            dc("unpause", victim_service)
            t_thaw = time.time()
        out, _ = watcher.communicate(timeout=seconds + 180)
        if watcher.returncode != 0 or not out.strip():
            sys.exit("the watcher in the client container failed (its error is above)")
        result = json.loads(out.strip().splitlines()[-1])
        summary = analyse(result, args.mode, victim_addr, t_event, t_thaw)
        summary["profile"] = profile
        summary["netem"] = dict(zip(("delay_ms", "jitter_ms", "loss_pct"), PROFILES[profile]))
        runs.append({"summary": summary, "raw": result})
        w = summary["writer"]
        line = ("%-9s false PFAIL %2d, FAIL %d | detected %5s s, agreed %5s s, promoted %5s s, ok %5s s | "
                "writes acked %6d lost %d uncertain %d, longest stall %.2fs") % (
            profile, summary["false_pfail_events"], summary["false_fail_events"], summary["detected_s"],
            summary["agreed_fail_s"], summary["replica_promoted_s"], summary["cluster_ok_again_s"],
            w["acked"], w["lost"], w["uncertain"], w["longest_stall_s"])
        if args.mode == "pause":
            line += " | after thaw: demoted %s s, stale writes accepted %d (survived: %s), first reply %s" % (
                summary["victim_demoted_s"], summary["stale_writes_accepted_after_thaw"],
                summary["stale_accepted_write_survived"], summary["stale_first_reply_after_thaw"])
        print(line, flush=True)
    for n in NODES:
        if n != "node2":
            shape(n)
    save("netem-failover-%s" % args.mode, {"meta": {"node_timeout_ms": 2000, "quiet_s": args.quiet},
                                            "runs": runs})


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    lat = sub.add_parser("latency")
    lat.add_argument("--rtt", type=float, nargs="+", default=[0, 0.5, 1, 2, 5], help="added round trip, ms")
    lat.add_argument("--duration", type=float, default=5)
    fo = sub.add_parser("failover")
    fo.add_argument("--profiles", nargs="+", default=["none", "cross-az", "lossy", "bad"], choices=PROFILES)
    fo.add_argument("--mode", choices=["kill", "pause"], default="kill")
    fo.add_argument("--quiet", type=float, default=20, help="seconds of normal running before the failure")
    fo.add_argument("--pause", type=float, default=8, help="pause mode: seconds the master stays frozen")
    fo.add_argument("--after", type=float, default=12, help="seconds watched after the failure (or thaw)")
    args = p.parse_args()
    if not os.path.exists("/proc"):
        sys.exit("run this from Linux/WSL")
    {"latency": cmd_latency, "failover": cmd_failover}[args.cmd](args)


if __name__ == "__main__":
    main()
