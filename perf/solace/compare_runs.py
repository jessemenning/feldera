#!/usr/bin/env python3
"""Tabulate multiple results JSON files side by side.

Usage: python compare_runs.py results/a.json results/b.json ...
"""

import json
import sys

ROWS = [
    ("label", lambda r: r["run"]["label"]),
    ("git_sha", lambda r: r["run"]["git_sha"]),
    ("rate", lambda r: r["params"]["rate"]),
    ("duration_s", lambda r: r["params"]["duration_s"]),
    ("payload_bytes", lambda r: r["params"]["payload_bytes"]),
    ("publishers", lambda r: r["params"]["publishers"]),
    ("window_size", lambda r: r["params"]["window_size"]),
    ("delivery_mode", lambda r: r["params"]["delivery_mode"]),
    ("preload", lambda r: r["params"]["preload"]),
    ("achieved_rate", lambda r: (r["publish"] or {}).get("achieved_rate")),
    ("ingest_mean/s", lambda r: r["throughput"]["steady_ingest_rate_mean"]),
    ("drain/s", lambda r: r["throughput"]["drain_rate"]),
    ("lat_p50_ms", lambda r: r["latency_e2e_ms"].get("p50")),
    ("lat_p95_ms", lambda r: r["latency_e2e_ms"].get("p95")),
    ("lat_p99_ms", lambda r: r["latency_e2e_ms"].get("p99")),
    ("lat_max_ms", lambda r: r["latency_e2e_ms"].get("max")),
    ("vis_lag_avg_ms", lambda r: r["visible_lag_ms"]["avg"]),
    ("catchup_proc_s", lambda r: r["catch_up_s"]["processed"]),
    ("catchup_echo_s", lambda r: r["catch_up_s"]["echo"]),
    ("q_backlog_max", lambda r: r["backlog"]["q_backlog_max"]),
    ("seq_gaps", lambda r: r["checks"]["seq_gaps"]),
    ("termination", lambda r: r["checks"]["termination"]),
]


def main(paths: list[str]) -> int:
    if not paths:
        print(__doc__)
        return 1
    runs = []
    for path in paths:
        with open(path) as f:
            r = json.load(f)
        if r.get("schema_version") != 1:
            print(f"warning: {path} has schema_version {r.get('schema_version')}")
        runs.append(r)

    width = max(14, *(len(str(fn(r))) for _, fn in ROWS for r in runs)) + 2
    label_w = max(len(name) for name, _ in ROWS) + 2
    for name, fn in ROWS:
        cells = "".join(f"{str(fn(r)):>{width}}" for r in runs)
        print(f"{name:<{label_w}}{cells}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
