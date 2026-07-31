# Solace Connector Performance Test

Measures throughput and end-to-end latency of the Feldera Solace Platform
connector (`solace_input` / `solace_output`). The methodology is modeled on
the perf test in `materialize-solace/test/solace/`, with these improvements:

- **True per-message latency percentiles** (p50/p90/p95/p99/p99.9) from an
  egress sink consumer, instead of poll-sampled visible lag.
- **Consumed-side throughput**: rows/sec derived from Feldera's
  `total_processed_records`, not just the publisher's achieved rate.
- **Machine-readable results**: one JSON file per run under `results/`,
  including all parameters and the raw sample series.
- **Parameterized message size** (`--payload-bytes`) and a multi-connection
  publisher (`--publishers N`).
- **Preload/drain mode** (`--preload`): pure ingest ceiling with the
  publisher out of the loop.

## Prerequisites

- Docker with compose v2, Python >= 3.10.
- `vm.max_map_count >= 512000` — the broker's internal services fail without
  it. `run.sh` raises it automatically when it can; on WSL2 make it
  persistent inside the distro:

  ```
  echo 'vm.max_map_count=512000' | sudo tee /etc/sysctl.d/99-solace.conf
  sudo sysctl --system     # then: wsl --shutdown (from Windows)
  ```

- The Feldera image with the Solace connector:
  `docker pull ghcr.io/jessemenning/feldera:feat-solace-connector`
  (override with `FELDERA_IMAGE=...`).

## Quick start

```bash
./run.sh --label smoke --rate 50 --duration 5      # smoke test
./run.sh --label baseline --rate 500 --duration 600
./run.sh down                                      # tear down the stack
```

`run.sh` brings up the compose stack (first broker start takes minutes; the
healthcheck allows 300 s), creates a venv, installs the in-repo Feldera SDK,
and executes `python -m solace_perf` with your arguments.

## Topology

```
publisher threads ──SMF──► [Solace broker] feldera_perf_q
                                              │  solace_input
                                              ▼
                            [Feldera] events ─► mv1 ─► mv2 ─► mv3 (polled)
                                              │ echo view
                                              ▼  solace_output
sink listener ◄──SMF── feldera_perf_echo_q ◄── topic feldera/perf/echo
```

- The publisher stamps `send_ts_ms` into each payload and publishes
  persistent messages to the queue via the `#P2P/QUE/<queue>` topic.
- The pipeline is a three-deep materialized-view cascade (projection →
  per-second aggregate → global rollup) so the circuit does non-trivial
  incremental work, mirroring the materialize reference.
- The `echo` view sends `(seq, send_ts_ms)` back out through the
  `solace_output` sink; the listener computes `recv_ts - send_ts` per
  message. Publisher and listener share one process and host clock, so the
  difference is skew-free.

## The three lag surfaces

| Metric | Source | Meaning |
|---|---|---|
| `q_backlog` | SEMP monitor: `lastSpooledMsgId - highestAckedMsgId` | Messages the broker still considers un-acked (broker → connector) |
| `buffered_input_records` | Feldera `/stats` | Parsed by the connector but not yet in a completed circuit step |
| e2e latency | sink listener | Publish → broker → connector → circuit → sink → broker → listener |

`spooledMsgCount` is **not** used for backlog: it is cumulative and never
decrements as messages are consumed, so a fully drained queue still reports
the total ever spooled.

**Ack-timing caveat:** what `q_backlog` covers depends on the connector
build. Builds that ack immediately after handing messages to the circuit
show a backlog near zero even while records are still buffered in-circuit;
builds that defer acks to step completion include in-circuit messages in the
backlog. Results JSON records `git_sha` and `feldera_image` so runs are
attributable to a connector build — compare like with like.

`visible_lag_ms` (wall clock minus the newest `send_ts_ms` visible in mv3,
sampled once per poll) is reported only for comparison with the
materialize-solace harness; it carries up to a full poll interval of jitter
by construction. The sink percentiles are the authoritative latency numbers.

## CLI reference

```
--rate 500              target msg/s (aggregate across publishers)
--duration 60           seconds of publishing
--payload-bytes 0       target serialized size; 0 = minimal (~45 B)
--publishers 1          publisher connections/threads
--window-size 255       solace_input flow window
--delivery-mode direct  sink delivery: direct | persistent
--with-sink/--no-sink   sink path on (default) or off
--preload               paused-pipeline preload + drain mode
--pipeline-workers 4    Feldera runtime workers
--poll-interval 1.0     sampler cadence (s)
--feldera-url http://localhost:8080
--semp-url    http://localhost:8088
--smf-host localhost --smf-port 55555
--broker-host-internal solace    broker hostname as Feldera sees it
--label run             results filename prefix
--results-dir results/
--keep-pipeline         leave the pipeline running after the run
```

## Standard run matrix

| Run | Command | Purpose |
|---|---|---|
| baseline | `./run.sh --label baseline --rate 500 --duration 600` | 300 k msgs steady state; headline p99 |
| burst | `./run.sh --label burst --rate 5000 --duration 60 --publishers 4` | Backlog growth + catch-up time |
| drain | `./run.sh --label drain --preload --rate 5000 --duration 60 --publishers 4` | Pure ingest throughput ceiling |
| payload 1 KB | `./run.sh --label payload1k --rate 500 --duration 300 --payload-bytes 1024` | Message-size sensitivity |
| persistent sink | `./run.sh --label persistent --rate 500 --duration 300 --delivery-mode persistent` | Sink ack cost vs baseline |
| window sweep | `./run.sh --label win50 --window-size 50 --rate 2000 --duration 120` (also 255, 1000) | Flow-window tuning curve |

## Interpreting output

During a run, one table row prints per poll interval with two phases:
`PUBLISHING` (load is being offered) and `CATCH-UP` (publisher done, waiting
for the pipeline and sink to drain). The run terminates when
`total_processed_records`, `mv3_total`, and (with sink) `echo_received` all
reach the published total, or on timeout.

The final `PERFORMANCE REPORT` block and the results JSON contain:

- publish: target vs achieved rate, errors, worst pacing deficit
- throughput: steady-state ingest rate (PUBLISHING phase, first/last 5 s
  trimmed), drain rate for `--preload`
- e2e latency percentiles + duplicate count (at-least-once means
  redeliveries are possible; only the first receipt per `seq` is counted)
- catch-up seconds per stage (linearly interpolated between samples)
- correctness checks: `total_input_records == published`, sequence gaps

Compare runs:

```bash
python compare_runs.py results/baseline_*.json results/persistent_*.json
```
