# Baseline perf results

Committed reference runs so future connector changes have a regression gate.
`results/` is gitignored (scratch); this directory is tracked.

## 2026-08-05 — pre/post code-review A/B

Compares the connector before the review remediation (`a4883be8c`) against
after (`2be48bc51`). Both runs used the same broker
(`solace/solace-pubsub-standard:10.10`) and host; the two builds were
selected via `FELDERA_IMAGE`. Reproduce:

```bash
FELDERA_IMAGE=ghcr.io/jessemenning/feldera:<sha> ./run.sh --label <l> --rate 500 --duration 120
FELDERA_IMAGE=ghcr.io/jessemenning/feldera:<sha> ./run.sh --label <l> --preload --rate 5000 --duration 60 --publishers 4
python compare_runs.py results-baseline/pre-review_*.json results-baseline/post-review_*.json
```

| Run | pre-review (`a4883be8c`) | post-review (`2be48bc51`) |
|-----|--------------------------|---------------------------|
| steady ingest @ 500/s | 500.0/s | 500.0/s |
| e2e p95 | 62 ms | 55 ms |
| e2e p99 | 72 ms | 71 ms |
| e2e p99.9 / max | 194 ms | 114 ms |
| drain ceiling (preload 5000/s ×4) | 36,867/s | 37,033/s |
| drain buffered-input max | 581 | 377 |
| correctness (input==published) | ✓ | ✓ |

**Verdict:** no throughput regression. At 500/s the pipeline is unsaturated,
so throughput is identical and only tail latency differs (post-review's tail
is modestly tighter). At the drain ceiling both builds land within 0.4%
(noise) — the 3-deep MV cascade is the bottleneck, not the connector — but
post-review buffers less input, consistent with the batch-drain change. The
input-path allocation savings (metadata gate, raw-RGMID cache) reduce memory
pressure without moving this compute-bound ceiling.

Note: the harness stamps the host repo's HEAD as `run.git_sha`, not the
image's build SHA, so `compare_runs.py` shows the same git_sha for both. The
build under test is set by `FELDERA_IMAGE` (recorded in the table above).
