#!/usr/bin/env python3
"""Run the runtime-arm matrix on gb300, interleaved, with a host record per run.

Before each timed run: busy CPU over 3 s (from /proc/stat), the processes that
used CPU in those 3 s, and the load average. During each run: CPU per PID
from /proc/<pid>/stat before and after; a process outside this runner's own
tree that used >= FOREIGN_MS of CPU flags the run, and the run is repeated
(at most twice). Every run's output and record goes to OUT.

usage: run_matrix.py OUT {smoke|timed|stall} REPS [cell,cell,...]
"""
import json, os, re, subprocess, sys, time

ST = os.environ["BB_THREAD_STORAGE"]
BIN = f"{ST}/target/release/examples"
STALL_BIN = os.environ.get("STALL_BIN", "")
TASKSET = "8-35"
APP_CPUS = "8-11"
VELO_CPUS = "12-35"
ARMS = ["A", "B1", "B2", "C1", "C2", "C4", "D"]
FOREIGN_MS = 100
HZ = os.sysconf("SC_CLK_TCK")

PIN = f"--app-threads 4 --app-cpus {APP_CPUS} --velo-cpus {VELO_CPUS}"
RPB = f"{BIN}/response_plane_bench --credit-sweep-interval-ms 200 --flush-policy auto"
CELLS = {
    "closed1": f"{RPB} --load closed --streams 1 --records 20000 --warmup-records 2000",
    "closed64": f"{RPB} --load closed --streams 64 --records 2000 --warmup-records 200",
    "closed64p4": f"{RPB} --load closed --streams 64 --peers 4 --records 2000 --warmup-records 200",
    "flood64": f"{RPB} --load flood --streams 64 --records 5000 --warmup-records 500",
    "satclosed1": f"{RPB} --load closed --streams 1 --records 10000 --warmup-records 1000 --saturate 8",
    "satclosed64": f"{RPB} --load closed --streams 64 --records 2000 --warmup-records 200 --saturate 8",
    "unary1": f"{BIN}/tx_budget --rungs l4t --sizes 1024 --inflight 1 --min-iters 20000 --max-iters 20000",
    "unary64": f"{BIN}/tx_budget --rungs l4t --sizes 1024 --inflight 64 --min-iters 64000 --max-iters 64000",
    "satunary1": f"{BIN}/tx_budget --rungs l4t --sizes 1024 --inflight 1 --min-iters 5000 --max-iters 5000 --saturate 8",
    "satunary64": f"{BIN}/tx_budget --rungs l4t --sizes 1024 --inflight 64 --min-iters 32000 --max-iters 32000 --saturate 8",
    "paced": f"{BIN}/response_plane_bench --anchor-hosts 2 --engines 8 --requests 2000 --credit-sweep-interval-ms 200 --warmup-requests 200 --json",
    "batched": f"{BIN}/batched_streaming --engines 2 --requests 96 --max-batch 32 --tokens 40 --flush-policy auto --initial-credit 256",
}
SMOKE = {
    "closed1": " --records 500 --warmup-records 50",
    "closed64": " --records 100 --warmup-records 10",
    "closed64p4": " --records 100 --warmup-records 10",
    "flood64": " --records 200 --warmup-records 20",
    "satclosed1": " --records 300 --warmup-records 30",
    "satclosed64": " --records 100 --warmup-records 10",
    "unary1": " --min-iters 300 --max-iters 300",
    "unary64": " --min-iters 640 --max-iters 640",
    "satunary1": " --min-iters 300 --max-iters 300",
    "satunary64": " --min-iters 640 --max-iters 640",
    "paced": " --requests 300 --warmup-requests 30",
    "batched": "",
}


def cpu_times():
    with open("/proc/stat") as f:
        v = [int(x) for x in f.readline().split()[1:]]
    idle = v[3] + v[4]
    return sum(v), idle


def pid_ticks():
    out = {}
    for p in os.listdir("/proc"):
        if not p.isdigit():
            continue
        try:
            with open(f"/proc/{p}/stat") as f:
                s = f.read()
            comm = s[s.index("(") + 1 : s.rindex(")")]
            rest = s[s.rindex(")") + 2 :].split()
            out[int(p)] = (comm, int(rest[11]) + int(rest[12]), int(rest[1]))
        except (OSError, ValueError, IndexError):
            pass
    return out


def mine():
    """This runner and its ancestors (the shell, the agent that started it)."""
    pids = set()
    p = os.getpid()
    while p > 1:
        pids.add(p)
        try:
            with open(f"/proc/{p}/stat") as f:
                s = f.read()
            p = int(s[s.rindex(")") + 2 :].split()[1])
        except OSError:
            break
    return pids


def is_kernel(pid):
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as f:
            return f.read() == b""
    except OSError:
        return True


def delta(before, after, exclude, min_ms):
    used = []
    for pid, (comm, t, _) in after.items():
        if pid in exclude:
            continue
        t0 = before.get(pid, (comm, 0, 0))[1] if pid in before else 0
        ms = (t - t0) * 1000 / HZ
        if ms >= min_ms:
            used.append({"pid": pid, "comm": comm, "ms": ms, "kernel": is_kernel(pid)})
    return sorted(used, key=lambda x: -x["ms"])


def precheck(exclude):
    t0, i0 = cpu_times()
    p0 = pid_ticks()
    time.sleep(3)
    t1, i1 = cpu_times()
    p1 = pid_ticks()
    busy = (1 - (i1 - i0) / max(1, t1 - t0)) * os.cpu_count()
    with open("/proc/loadavg") as f:
        load = f.read().split()[:3]
    return {
        "busy_cores_3s": round(busy, 2),
        "loadavg": load,
        "active_3s": delta(p0, p1, exclude, 10),
    }, p1


def run_one(out, cell, arm, rep, phase, attempt):
    exclude = mine()
    if phase == "stall":
        cmd = f"taskset -c {TASKSET} {STALL_BIN} --ignored --test-threads=1 --nocapture"
        env = dict(os.environ, VELO_ARM=arm, STALL_N="50")
        tmo = 900
    else:
        base = CELLS[cell]
        if phase == "smoke":
            o = SMOKE[cell].split()
            for flag, val in zip(o[::2], o[1::2]):
                base = re.sub(rf"{flag} \S+", f"{flag} {val}", base)
        cmd = f"taskset -c {TASKSET} {base} --arm {arm} {PIN}"
        env = dict(os.environ)
        tmo = 300
    pre, before = precheck(exclude) if phase != "smoke" else ({}, pid_ticks())
    t = time.time()
    r = subprocess.run(cmd.split(), stdin=subprocess.DEVNULL, capture_output=True, text=True, env=env, timeout=tmo)
    wall = time.time() - t
    after = pid_ticks()
    # The child is gone; its PID cannot be in `after` unless reused.
    foreign = delta(before, after, exclude, FOREIGN_MS)
    flagged = [f for f in foreign if not f["kernel"]]
    name = f"{cell}-{arm}-r{rep}" + (f"-a{attempt}" if attempt else "")
    os.makedirs(f"{out}/raw", exist_ok=True)
    with open(f"{out}/raw/{name}.txt", "w") as f:
        f.write(f"$ {cmd}\n{r.stdout}\n--- stderr ---\n{r.stderr}")
    lines = [l for l in r.stdout.splitlines() if l.startswith(("ARM,", "STALL,"))]
    rec = {
        "cell": cell, "arm": arm, "rep": rep, "attempt": attempt, "phase": phase,
        "rc": r.returncode, "wall_s": round(wall, 2), "start": t, "pre": pre,
        "foreign_during": foreign, "flagged": flagged, "lines": lines,
    }
    with open(f"{out}/runs.jsonl", "a") as f:
        f.write(json.dumps(rec) + "\n")
    status = "ok" if r.returncode == 0 and lines else f"FAIL rc={r.returncode}"
    print(f"{time.strftime('%H:%M:%S')} {name} {status} {wall:.1f}s busy={pre.get('busy_cores_3s')} flagged={[(x['comm'], int(x['ms'])) for x in flagged]}", flush=True)
    return rec


def main():
    out, phase, reps = sys.argv[1], sys.argv[2], int(sys.argv[3])
    cells = sys.argv[4].split(",") if len(sys.argv) > 4 else list(CELLS)
    os.makedirs(out, exist_ok=True)
    git = subprocess.run(["git", "-C", os.environ["WORKTREE"], "describe", "--always", "--dirty"], capture_output=True, text=True).stdout.strip()
    with open(f"{out}/meta.json", "w") as f:
        json.dump({"git": git, "host": os.uname().nodename, "nproc": os.cpu_count(), "taskset": TASKSET,
                   "app_cpus": APP_CPUS, "velo_cpus": VELO_CPUS, "cells": CELLS, "arms": ARMS, "phase": phase,
                   "reps": reps, "kernel": os.uname().release}, f, indent=1)
    if phase == "stall":
        for i in range(len(ARMS)):
            arm = ARMS[(i + reps) % len(ARMS)]
            run_one(out, "stall", arm, 0, phase, 0)
        return
    for rep in range(reps):
        for ci, cell in enumerate(cells):
            k = (rep + ci) % len(ARMS)
            for arm in ARMS[k:] + ARMS[:k]:
                for attempt in range(3):
                    rec = run_one(out, cell, arm, rep, phase, attempt)
                    if phase == "smoke" or not rec["flagged"]:
                        break
                    print(f"  rerun: foreign CPU during {cell}/{arm}/r{rep}", flush=True)


if __name__ == "__main__":
    main()
