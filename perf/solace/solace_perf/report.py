"""Summary report and machine-readable results JSON."""

import json
import logging
import os
import statistics
import subprocess
import time
from datetime import datetime, timezone

from .config import RESULTS_SCHEMA_VERSION, RunConfig
from .publisher import PublisherStats
from .sampler import SampleSet
from .sink_listener import SinkListener

log = logging.getLogger(__name__)


def _percentiles(values: list[int]) -> dict:
    """min/mean/p50/p90/p95/p99/p99.9/max over latency samples."""
    if not values:
        return {"count": 0}
    values = sorted(values)
    n = len(values)

    def pct(p: float) -> int:
        return values[min(n - 1, int(p * n))]

    return {
        "count": n,
        "min": values[0],
        "mean": round(statistics.mean(values), 1),
        "p50": pct(0.50),
        "p90": pct(0.90),
        "p95": pct(0.95),
        "p99": pct(0.99),
        "p999": pct(0.999),
        "max": values[-1],
    }


def _git_sha() -> str:
    try:
        return (
            subprocess.run(
                ["git", "rev-parse", "--short", "HEAD"],
                capture_output=True,
                text=True,
                check=True,
            ).stdout.strip()
        )
    except Exception:
        return "unknown"


def build_results(
    cfg: RunConfig,
    pub_stats: PublisherStats | None,
    samples: SampleSet,
    listener: SinkListener | None,
    total_expected: int,
    drain_rate: float | None,
) -> dict:
    """Assemble the full results document (schema v1)."""
    publish_end_t = None
    for s in samples.samples:
        if s["phase"] == "CATCH-UP":
            publish_end_t = s["t"]
            break
    if publish_end_t is None and samples.samples:
        publish_end_t = samples.samples[-1]["t"]

    catch_up = {
        "processed": samples.catch_up_seconds(
            "total_processed_records", total_expected, publish_end_t or 0.0
        ),
        "mv3": samples.catch_up_seconds("mv3_total", total_expected, publish_end_t or 0.0),
        "echo": (
            samples.catch_up_seconds("echo_received", total_expected, publish_end_t or 0.0)
            if listener
            else None
        ),
    }

    # Steady-state ingest rate over the PUBLISHING phase, trimming the first
    # and last 5 seconds of warmup/rampdown.
    pub_samples = [s for s in samples.samples if s["phase"] == "PUBLISHING"]
    if pub_samples:
        t_lo = pub_samples[0]["t"] + 5
        t_hi = pub_samples[-1]["t"] - 5
        rates = [
            s["ingest_rate"] for s in pub_samples if t_lo <= s["t"] <= t_hi
        ] or [s["ingest_rate"] for s in pub_samples]
    else:
        rates = []

    visible_lags = [
        s["visible_lag_ms"]
        for s in samples.samples
        if s["phase"] == "CATCH-UP" and 0 < s["visible_lag_ms"] < 60_000
    ]

    latency = _percentiles(listener.latencies_ms()) if listener else {"count": 0}
    if listener:
        latency["duplicates"] = listener.duplicate_count
        latency["decode_errors"] = listener.decode_errors

    max_backlog = max((s["q_backlog"] for s in samples.samples), default=-1)
    max_buffered = max(
        (s["buffered_input_records"] for s in samples.samples), default=-1
    )
    last = samples.samples[-1] if samples.samples else {}

    return {
        "schema_version": RESULTS_SCHEMA_VERSION,
        "run": {
            "label": cfg.label,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "git_sha": _git_sha(),
            "feldera_image": os.environ.get(
                "FELDERA_IMAGE", "ghcr.io/jessemenning/feldera:feat-solace-connector"
            ),
            "solace_tag": os.environ.get("SOLACE_TAG", "10.10"),
            "host": os.uname().nodename,
        },
        "params": cfg.params_dict(),
        "publish": (
            {
                "attempted": pub_stats.attempted,
                "published": pub_stats.published,
                "errors": pub_stats.errors,
                "duration_s": round(pub_stats.duration_s, 2),
                "achieved_rate": round(pub_stats.achieved_rate, 1),
                "max_pacing_deficit_ms": round(pub_stats.max_pacing_deficit_ms, 1),
            }
            if pub_stats
            else None
        ),
        "throughput": {
            "steady_ingest_rate_mean": round(statistics.mean(rates), 1) if rates else None,
            "steady_ingest_rate_max": max(rates) if rates else None,
            "drain_rate": round(drain_rate, 1) if drain_rate else None,
        },
        "latency_e2e_ms": latency,
        "visible_lag_ms": {
            "avg": round(statistics.mean(visible_lags), 1) if visible_lags else None,
            "median": statistics.median(visible_lags) if visible_lags else None,
            "max": max(visible_lags) if visible_lags else None,
        },
        "catch_up_s": catch_up,
        "backlog": {
            "q_backlog_max": max_backlog,
            "buffered_input_records_max": max_buffered,
        },
        "checks": {
            "input_records_match": last.get("total_input_records") == total_expected,
            "seq_gaps": listener.seq_gaps(total_expected) if listener else None,
            "termination": samples.termination,
        },
        "samples": samples.samples,
    }


def print_summary(results: dict) -> None:
    p = results["params"]
    pub = results["publish"]
    thr = results["throughput"]
    lat = results["latency_e2e_ms"]
    vis = results["visible_lag_ms"]
    cu = results["catch_up_s"]
    bl = results["backlog"]
    ck = results["checks"]

    def fmt(v, suffix=""):
        return f"{v}{suffix}" if v is not None else "n/a"

    print("\n" + "=" * 72)
    print(f"PERFORMANCE REPORT — {results['run']['label']}")
    print("=" * 72)
    if pub:
        print(
            f"publish:    target {p['rate']}/s x {p['duration_s']}s, achieved "
            f"{pub['achieved_rate']}/s, {pub['published']}/{pub['attempted']} sent, "
            f"{pub['errors']} errors, worst pacing deficit {pub['max_pacing_deficit_ms']}ms"
        )
    print(
        f"throughput: steady ingest {fmt(thr['steady_ingest_rate_mean'], '/s')} "
        f"(max {fmt(thr['steady_ingest_rate_max'], '/s')})"
        + (f", drain {thr['drain_rate']}/s" if thr["drain_rate"] else "")
    )
    if lat.get("count"):
        print(
            f"e2e latency (sink, {lat['count']} samples): "
            f"min {lat['min']}  mean {lat['mean']}  p50 {lat['p50']}  "
            f"p90 {lat['p90']}  p95 {lat['p95']}  p99 {lat['p99']}  "
            f"p99.9 {lat['p999']}  max {lat['max']}  [ms]  "
            f"dups {lat['duplicates']}"
        )
    print(
        f"visible lag (poll-jittered, materialize-comparison only): "
        f"avg {fmt(vis['avg'])}  median {fmt(vis['median'])}  max {fmt(vis['max'])} [ms]"
    )
    print(
        f"catch-up after last publish: processed {fmt(cu['processed'], 's')}, "
        f"mv3 {fmt(cu['mv3'], 's')}, echo {fmt(cu['echo'], 's')}"
    )
    print(
        f"backlog:    broker queue max {bl['q_backlog_max']}, "
        f"buffered input max {bl['buffered_input_records_max']}"
    )
    print(
        f"checks:     input==published: {ck['input_records_match']}, "
        f"seq gaps: {ck['seq_gaps']}, termination: {ck['termination']}"
    )
    print("=" * 72)


def write_results(results: dict, cfg: RunConfig) -> str:
    os.makedirs(cfg.results_dir, exist_ok=True)
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    path = os.path.join(cfg.results_dir, f"{cfg.label}_{stamp}.json")
    with open(path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults written to {path}")
    return path
