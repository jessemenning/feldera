"""Measurement loop: samples Feldera stats, SEMP backlog, and visible lag.

Runs in the main thread once per ``poll_interval``. Each sample is one dict
appended to ``samples`` and printed as one row of the live table.

Three lag surfaces are sampled (see README for interpretation):
- SEMP ``q_backlog``: messages the broker has not yet had acked.
- ``buffered_input_records``: parsed by the connector, not yet in a
  completed circuit step.
- ``visible_lag_ms``: wall clock minus the newest ``send_ts_ms`` visible in
  mv3 -- poll-jittered by construction; kept for materialize comparison. The
  authoritative latency distribution comes from the sink listener.
"""

import logging
import time
from dataclasses import dataclass, field

from .broker import Semp
from .config import INGEST_QUEUE, RunConfig
from .pipeline import PerfPipeline
from .publisher import Publisher
from .sink_listener import SinkListener

log = logging.getLogger(__name__)

_HEADER = (
    f"{'phase':<10} {'t':>7} {'pub_sent':>9} {'input':>9} {'buffered':>9} "
    f"{'processed':>9} {'rate/s':>8} {'echo':>9} {'backlog':>8} {'delta':>7} "
    f"{'mv3_total':>9} {'lag_ms':>8} {'mv3_ms':>7}"
)


@dataclass
class SampleSet:
    samples: list[dict] = field(default_factory=list)
    termination: str = "timeout"

    def catch_up_seconds(self, key: str, target: int, publish_end_t: float):
        """First time (relative to publish end) ``key`` reached ``target``.

        Linearly interpolates between the straddling samples for sub-poll
        resolution. Returns None if the target was never reached.
        """
        prev = None
        for s in self.samples:
            v = s.get(key, -1)
            if v >= target:
                if prev is not None and v > prev.get(key, 0):
                    frac = (target - prev[key]) / (v - prev[key])
                    t = prev["t"] + frac * (s["t"] - prev["t"])
                else:
                    t = s["t"]
                return round(t - publish_end_t, 2)
            prev = s
        return None


class Sampler:
    def __init__(
        self,
        cfg: RunConfig,
        pipeline: PerfPipeline,
        semp: Semp,
        publisher: Publisher | None,
        listener: SinkListener | None,
    ):
        self.cfg = cfg
        self.pipeline = pipeline
        self.semp = semp
        self.publisher = publisher
        self.listener = listener

    def run(self, total_expected: int) -> SampleSet:
        """Poll until everything caught up or the timeout expired."""
        cfg = self.cfg
        result = SampleSet()
        print(_HEADER)

        start = time.monotonic()
        publish_end: float | None = None
        prev_backlog: int | None = None
        prev_processed: int | None = None
        prev_t: float | None = None
        # Generous tail: catch-up may include pipeline step cadence and
        # adhoc-query latency.
        tail_budget = max(120.0, 4 * cfg.poll_interval_s) + cfg.duration_s / 10
        last_poll = start

        while True:
            sleep_for = max(0.0, cfg.poll_interval_s - (time.monotonic() - last_poll))
            time.sleep(sleep_for)
            last_poll = time.monotonic()
            t = last_poll - start

            publishing = self.publisher is not None and self.publisher.is_alive()
            if not publishing and publish_end is None:
                publish_end = t

            sample = {"t": round(t, 2), "phase": "PUBLISHING" if publishing else "CATCH-UP"}

            if self.publisher is not None:
                with self.publisher.stats.lock:
                    sample["pub_sent"] = self.publisher.stats.published
            else:
                sample["pub_sent"] = total_expected

            try:
                gm = self.pipeline.global_metrics()
                sample["total_input_records"] = gm.total_input_records or 0
                sample["buffered_input_records"] = gm.buffered_input_records or 0
                sample["total_processed_records"] = gm.total_processed_records or 0
            except Exception as e:
                log.debug("stats poll failed: %s", e)
                sample["total_input_records"] = -1
                sample["buffered_input_records"] = -1
                sample["total_processed_records"] = -1

            processed = sample["total_processed_records"]
            if prev_processed is not None and processed >= 0 and prev_t is not None:
                dt = t - prev_t
                sample["ingest_rate"] = (
                    round((processed - prev_processed) / dt, 1) if dt > 0 else 0.0
                )
            else:
                sample["ingest_rate"] = 0.0
            if processed >= 0:
                prev_processed, prev_t = processed, t

            sample["echo_received"] = (
                self.listener.received_count if self.listener else -1
            )

            qs = self.semp.queue_stats(INGEST_QUEUE)
            sample["q_backlog"] = qs["q_backlog"]
            sample["q_delta"] = (
                qs["q_backlog"] - prev_backlog
                if prev_backlog is not None and qs["q_backlog"] >= 0
                else 0
            )
            if qs["q_backlog"] >= 0:
                prev_backlog = qs["q_backlog"]

            mv3_t0 = time.monotonic()
            try:
                row = self.pipeline.query_mv3()
                sample["mv3_ms"] = int((time.monotonic() - mv3_t0) * 1000)
                total = row.get("total_msgs")
                latest_ts = row.get("latest_send_ts_ms")
                sample["mv3_total"] = int(total) if total is not None else 0
                sample["visible_lag_ms"] = (
                    int(time.time() * 1000) - int(latest_ts)
                    if latest_ts
                    else -1
                )
            except Exception as e:
                log.debug("mv3 query failed: %s", e)
                sample["mv3_ms"] = -1
                sample["mv3_total"] = -1
                sample["visible_lag_ms"] = -1

            result.samples.append(sample)
            self._print_row(sample)

            done = (
                not publishing
                and sample["total_processed_records"] >= total_expected
                and sample["mv3_total"] >= total_expected
                and (
                    self.listener is None
                    or sample["echo_received"] >= total_expected
                )
            )
            if done:
                result.termination = "complete"
                break
            if publish_end is not None and t > publish_end + tail_budget:
                result.termination = "timeout"
                break

        return result

    @staticmethod
    def _print_row(s: dict) -> None:
        print(
            f"{s['phase']:<10} {s['t']:>7.1f} {s['pub_sent']:>9} "
            f"{s['total_input_records']:>9} {s['buffered_input_records']:>9} "
            f"{s['total_processed_records']:>9} {s['ingest_rate']:>8.1f} "
            f"{s['echo_received']:>9} {s['q_backlog']:>8} {s['q_delta']:>7} "
            f"{s['mv3_total']:>9} {s['visible_lag_ms']:>8} {s['mv3_ms']:>7}"
        )
