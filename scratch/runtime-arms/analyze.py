#!/usr/bin/env python3
"""Summarize runs.jsonl: per cell and arm, the median and range of each metric
over the reps (last attempt of each rep only). usage: analyze.py DIR"""
import json, statistics, sys
from collections import defaultdict

d = sys.argv[1]
runs = [json.loads(l) for l in open(f"{d}/runs.jsonl")]
# The last attempt of each (cell, arm, rep) is the one that counts.
final = {}
for r in runs:
    final[(r["cell"], r["arm"], r["rep"])] = r
flagged_reruns = [r for r in runs if r["flagged"]]

def parse(line):
    kv = {}
    for part in line.split(",")[1:]:
        if "=" in part:
            k, v = part.split("=", 1)
            try:
                kv[k] = float(v)
            except ValueError:
                kv[k] = v
    return kv

ARMS = ["A", "B1", "B2", "C1", "C2", "C4", "D"]
METRICS = {
    "closed": ["rec_per_s", "p50_us", "p99_us", "p999_us", "cpu_us_per_rec", "vcsw_per_rec", "wakes_per_rec", "rec_per_batch", "threads"],
    "flood": ["rec_per_s", "p50_us", "p99_us", "cpu_us_per_rec", "vcsw_per_rec", "wakes_per_rec", "rec_per_batch", "threads"],
    "l4t": ["calls_per_s", "p50_ns", "p99_ns", "p999_ns", "cpu_us_per_call", "vcsw_per_call", "threads"],
    "paced": ["cpu_us_per_token", "vcsw_per_token", "wakes_per_token", "tokens_per_write", "itl_p50_us", "itl_p99_us", "ttft_p50_us", "ttft_p99_us", "threads"],
    "batched_streaming": ["cpu_us_per_token", "vcsw_per_token", "wakes_per_token", "elapsed_ms", "threads"],
}

cells = defaultdict(lambda: defaultdict(list))
for (cell, arm, rep), r in final.items():
    if r["rc"] != 0:
        continue
    for line in r["lines"]:
        if line.startswith("ARM,"):
            cells[cell][arm].append(parse(line))

def kind(cell, sample):
    if "rung" in sample:
        return "l4t"
    if sample.get("bench") == "batched_streaming":
        return "batched_streaming"
    return sample.get("load")

out = []
for cell in cells:
    arms = cells[cell]
    any_sample = next(iter(arms.values()))[0]
    metrics = METRICS[kind(cell, any_sample)]
    out.append(f"\n## {cell}\n")
    out.append("| arm | n | " + " | ".join(metrics) + " |")
    out.append("|---|---|" + "---|" * len(metrics))
    for arm in ARMS:
        if arm not in arms:
            continue
        samples = arms[arm]
        row = [arm, str(len(samples))]
        for m in metrics:
            vals = [s[m] for s in samples if m in s]
            if not vals:
                row.append("-")
                continue
            med = statistics.median(vals)
            fmt = (lambda x: f"{x:.0f}") if med >= 100 else (lambda x: f"{x:.3g}")
            row.append(f"{fmt(med)} ({fmt(min(vals))}–{fmt(max(vals))})")
        out.append("| " + " | ".join(row) + " |")

stall = [r for r in final.values() if r["cell"] == "stall"]
if stall:
    out.append("\n## stall rows (rate, stalled-delay p50 µs)\n")
    rows = defaultdict(dict)
    for r in stall:
        for line in r["lines"]:
            s = parse(line)
            rows[f"{int(s['row'])} {s['what']}"][r["arm"]] = f"{s['stalls']:.0f}/{s['n']:.0f} ({s['stall_p50_us']:.0f})"
    out.append("| row | " + " | ".join(ARMS) + " |")
    out.append("|---|" + "---|" * len(ARMS))
    for row in sorted(rows):
        out.append(f"| {row} | " + " | ".join(rows[row].get(a, "-") for a in ARMS) + " |")

busy = [r["pre"].get("busy_cores_3s", 0) for r in final.values() if r.get("pre")]
if busy:
    out.append(f"\nbusy cores over the 3 s before each run: median {statistics.median(busy):.2f}, max {max(busy):.2f}, n {len(busy)}")
out.append(f"runs rerun for foreign CPU: {len(flagged_reruns)}")
for r in flagged_reruns:
    out.append(f"  {r['cell']}/{r['arm']}/r{r['rep']}/a{r['attempt']}: {[(f['comm'], int(f['ms'])) for f in r['flagged']]}")
fails = [k for k, r in final.items() if r["rc"] != 0]
out.append(f"failed runs: {fails}")
print("\n".join(out))
