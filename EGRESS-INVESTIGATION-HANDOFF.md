# Handoff: investigate in-process Solace `solace_output` egress dropping records

Paste everything below into a fresh session working in `/home/jmenning/feldera-solace`.

---

## Task

The in-process Solace **output** connector (`solace_output`) delivers almost
none of its records to the broker in the performance harness, while the
pipeline ingests and processes everything correctly. Find the root cause and
fix it. This is an egress-only bug; ingest is healthy.

## Repository / build

- Repo: `/home/jmenning/feldera-solace`, branch `feat/solace-connector`.
- In-process connector source:
  - `crates/adapters/src/transport/solace/output.rs` — `SolaceOutputEndpoint`
    (publish, `send_message`, `drain_pending_acks`, direct vs persistent).
  - `crates/adapters/src/format/json/output.rs` — JSON encoder, including the
    `skip_deletes` logic and the Solace `{field}`-topic buffer coercion.
  - `crates/feldera-types/src/transport/solace.rs` — output config.
- Perf harness: `perf/solace/` (`docker-compose.yml`, `solace_perf/`).
  - `solace_perf/pipeline.py` `_sink_connector()` builds the echo view's
    `solace_output` connector JSON: `update_format: insert_delete`,
    **`skip_deletes: true`**, `delivery_mode` from `--delivery-mode`.
- Pipeline image with the connector: `ghcr.io/jessemenning/feldera:feat-solace-connector`
  (8.6 GB, already pulled locally).

## The symptom (reproduced 2026-08-21, single WSL2 host)

Steady run: publish 60,000 msgs (500/s × 120 s) → queue → `solace_input` →
pipeline (`events → mv1 → mv2 → mv3`, plus `echo = SELECT seq, send_ts_ms FROM
mv1`) → `echo` view's `solace_output` → topic `feldera/perf/echo` → queue
`feldera_perf_echo_q` → sink listener.

| run | egress delivery | echo msgs the sink received | echo msgs ever spooled to the queue | termination |
|---|---|---:|---:|---|
| `baseline-clean` | direct | 792 / 60,000 | ~792 | timeout |
| `baseline-persistent` | persistent | 8 / 60,000 | **205** | timeout |

- Pipeline ingested all 60,000 (`total_input_records == 60000`,
  `total_processed_records == 60000`, `input==published: true`).
- The connector logged a clean connect —
  `dbsp_adapters::transport::solace::output: Output connected to Solace
  tcp://solace:55555 vpn=default topic=feldera/perf/echo` — and **no publish
  errors** anywhere in the pipeline container logs.
- Echo queue cumulative `spooledMsgCount` reached only ~205 (SEMP monitor), so
  the messages were **never published to the topic** — this is a publish-side
  loss, not a listener-side loss.
- This reproduces the committed 2026-08-05 baselines
  (`perf/solace/results-baseline/pre-review*.json`, `post-review*.json`: 509 and
  560 sink samples, ~59,400 sequence gaps, `timeout`).

## What is already ruled out

- **Broker, echo topic, queue subscription, and sink-listener code are fine.**
  A separate **sidecar** (`/home/jmenning/feldera-solace-sidecar`) reads the
  same `echo` view over `/egress` and publishes to the same topic/queue with
  the same listener, and delivered **all 60,000** under identical conditions.
- **Ingest is fine** (500/s steady; the preload/drain path, which has no sink,
  completes cleanly).
- **Not a connection failure** — the connector connects and logs no errors.

## Ranked hypotheses (test top-down)

1. **`skip_deletes` is dropping inserts, not just deletes.** Every failing run
   used `skip_deletes: true` on an append-only view. If the encoder in
   `crates/adapters/src/format/json/output.rs` mis-classifies inserts (or drops
   whole buffers) when `skip_deletes` is set, near-total loss follows. The
   sidecar used a *different* skip path (drops only `{"delete": …}` change
   records) and lost nothing — consistent with the in-process `skip_deletes`
   being the culprit. **First test:** rerun the in-process harness with
   `skip_deletes` removed (edit `_sink_connector` to drop it, or hard-code the
   echo connector without it) and see if delivery recovers.
2. **Persistent ack drain stalls after the first in-flight window.** 205 ≈ one
   `max_inflight_acks` window (256). If `publish_with_ack` receivers never
   resolve (broker not acking topic publishes with no durable endpoint), the
   first `batch_end` `drain_pending_acks` hits `ack_timeout_secs` (30 s) and
   errors — after which little else publishes. Persistent delivered *fewer*
   than direct, which fits a drain-stall. Check whether the batch-end error is
   surfaced or silently swallowed (the harness saw no error).
3. **Direct-mode publishes dropped by the C-SDK send buffer under load.**
   Direct is fire-and-forget; if the SDK's transmit buffer fills faster than it
   drains, messages are discarded with no error. 792/60,000 would be the
   fraction that fit. Check for SDK `can_send`/back-pressure handling in
   `send_message`.
4. **The echo output only ever sees a snapshot, not the change stream.** The
   `echo` view is materialized; confirm the output connector receives all
   60,000 change rows (not just an initial batch). Instrument `push_buffer`
   call count.

## Concrete first steps

1. Reproduce with instrumentation: run the harness, then compare the
   connector's `push_buffer` invocation count / bytes to 60,000. Add a temporary
   counter+log in `SolaceOutputEndpoint::push_buffer` / `publish`.
2. Isolate `skip_deletes`: one run with it, one without (hypothesis 1).
3. Isolate delivery mode: `--delivery-mode direct` vs `persistent` with
   `push_buffer` counts (hypotheses 2/3).
4. If `push_buffer` is called 60,000× but few reach the broker, the bug is in
   `send_message`/`drain_pending_acks`/SDK usage. If it's called ~205×, the bug
   is upstream in the encoder / change-stream (hypotheses 1/4).

## Reproduction commands

```bash
cd /home/jmenning/feldera-solace/perf/solace
# broken (as captured): steady with sink
./run.sh --label repro-direct     --rate 500 --duration 120 --delivery-mode direct
./run.sh --label repro-persistent --rate 500 --duration 120 --delivery-mode persistent
./run.sh down   # tear down when finished

# echo queue depth (publish-side evidence):
curl -s -u admin:admin \
  "http://localhost:8088/SEMP/v2/monitor/msgVpns/default/queues/feldera_perf_echo_q?select=spooledMsgCount" \
  | python3 -c "import sys,json;print(json.load(sys.stdin)['data'].get('spooledMsgCount'))"

# connector logs:
docker logs feldera-solace-perf-feldera-1 2>&1 | grep -i solace
```

The `solace-integration-test` suite
(`crates/adapters/src/transport/solace/test.rs`,
`cargo test -p dbsp_adapters --no-default-features --features
solace-integration-test`) already has an `output_*` test group and a SEMP
client — add a high-volume output test there once you can reproduce in a unit
test, so the fix is regression-covered.

## Success criteria

- Root cause identified and explained (which of the hypotheses, or something
  else).
- Fix delivers ≥ 99.9 % of echo messages in `repro-direct` and
  `repro-persistent` (`termination: complete`, sequence gaps ~0).
- A regression test in `transport/solace/test.rs` covering high-volume output.

## Context / cross-reference

Full A/B write-up (with the exact numbers above and the sidecar contrast):
`/home/jmenning/feldera-solace-sidecar/docs/PERF-AB-REPORT.md`. Raw in-process
result files: `/home/jmenning/feldera-solace-sidecar/perf/results-inprocess/`.
