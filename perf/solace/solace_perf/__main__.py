"""CLI entry point: orchestrates one performance run.

Order: SEMP provision -> pipeline create+start (or start_paused for
--preload) -> sink listener -> publisher -> sampler loop -> report + JSON ->
teardown.
"""

import argparse
import logging
import sys
import time

from .broker import Semp
from .config import INGEST_QUEUE, RunConfig
from .pipeline import PerfPipeline
from .publisher import Publisher
from .report import build_results, print_summary, write_results
from .sampler import Sampler
from .sink_listener import SinkListener

log = logging.getLogger(__name__)


def parse_args(argv: list[str]) -> RunConfig:
    p = argparse.ArgumentParser(
        prog="solace_perf",
        description="Performance test for the Feldera Solace Platform connector.",
    )
    p.add_argument("--rate", type=int, default=500, help="target msg/s (aggregate)")
    p.add_argument("--duration", type=int, default=60, help="publish duration (s)")
    p.add_argument(
        "--payload-bytes",
        type=int,
        default=0,
        help="target serialized message size; 0 = minimal (~45 B)",
    )
    p.add_argument("--publishers", type=int, default=1, help="publisher threads")
    p.add_argument("--window-size", type=int, default=255, help="solace_input flow window")
    p.add_argument(
        "--delivery-mode",
        choices=["direct", "persistent"],
        default="direct",
        help="sink delivery mode",
    )
    sink = p.add_mutually_exclusive_group()
    sink.add_argument("--with-sink", dest="with_sink", action="store_true", default=True)
    sink.add_argument("--no-sink", dest="with_sink", action="store_false")
    p.add_argument(
        "--preload",
        action="store_true",
        help="publish everything into the queue with the pipeline paused, then "
        "measure pure drain throughput",
    )
    p.add_argument("--pipeline-workers", type=int, default=4)
    p.add_argument("--poll-interval", type=float, default=1.0)
    p.add_argument("--feldera-url", default="http://localhost:8080")
    p.add_argument("--semp-url", default="http://localhost:8088")
    p.add_argument("--smf-host", default="localhost")
    p.add_argument("--smf-port", type=int, default=55555)
    p.add_argument(
        "--broker-host-internal",
        default="solace",
        help="broker hostname as seen by Feldera inside the compose network",
    )
    p.add_argument("--label", default="run", help="results filename prefix")
    p.add_argument("--results-dir", default="results")
    p.add_argument("--keep-pipeline", action="store_true")
    a = p.parse_args(argv)

    return RunConfig(
        rate=a.rate,
        duration_s=a.duration,
        payload_bytes=a.payload_bytes,
        publishers=a.publishers,
        window_size=a.window_size,
        delivery_mode=a.delivery_mode,
        with_sink=a.with_sink,
        preload=a.preload,
        pipeline_workers=a.pipeline_workers,
        poll_interval_s=a.poll_interval,
        feldera_url=a.feldera_url,
        semp_url=a.semp_url,
        smf_host=a.smf_host,
        smf_port=a.smf_port,
        broker_host_internal=a.broker_host_internal,
        label=a.label,
        results_dir=a.results_dir,
        keep_pipeline=a.keep_pipeline,
    )


def main(argv: list[str]) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    # The Solace Platform Python SDK is chatty at INFO.
    logging.getLogger("solace.messaging").setLevel(logging.WARNING)

    cfg = parse_args(argv)
    total = cfg.total_messages
    print(
        f"Run '{cfg.label}': {cfg.rate}/s x {cfg.duration_s}s = {total} messages, "
        f"payload~{cfg.payload_bytes or 45}B, publishers={cfg.publishers}, "
        f"window={cfg.window_size}, sink={cfg.with_sink} ({cfg.delivery_mode}), "
        f"preload={cfg.preload}"
    )

    semp = Semp(cfg.semp_url)
    semp.provision(with_sink=cfg.with_sink)

    pipeline = PerfPipeline(cfg)
    pipeline.create()

    listener = None
    publisher = Publisher(cfg)
    drain_rate = None
    try:
        if cfg.preload:
            # Preload: pipeline paused, fill the queue, then time the drain.
            pipeline.start_paused()
            print("Pipeline paused; preloading queue...")
            publisher.start()
            publisher.join()
            backlog = semp.wait_for_backlog(INGEST_QUEUE, total)
            print(f"Preloaded {backlog} messages; resuming pipeline...")
            drain_start = time.monotonic()
            pipeline.resume()
            sampler = Sampler(cfg, pipeline, semp, publisher=None, listener=None)
            samples = sampler.run(total)
            drain_rate = (
                total / (time.monotonic() - drain_start)
                if samples.termination == "complete"
                else None
            )
        else:
            pipeline.start()
            if cfg.with_sink:
                listener = SinkListener(cfg)
                listener.start()
            publisher.start()
            sampler = Sampler(cfg, pipeline, semp, publisher, listener)
            samples = sampler.run(total)
            publisher.join(timeout_s=30)

        results = build_results(
            cfg, publisher.stats, samples, listener, total, drain_rate
        )
        print_summary(results)
        write_results(results, cfg)
        return 0 if samples.termination == "complete" else 1
    finally:
        if listener:
            listener.stop()
        pipeline.teardown()


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
