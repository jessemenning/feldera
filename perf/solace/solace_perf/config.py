"""Run configuration and constants shared across the harness."""

from dataclasses import dataclass, field

# Version of the results-JSON schema written by report.py. Bump when the
# structure changes so compare_runs.py can refuse to mix incompatible files.
RESULTS_SCHEMA_VERSION = 1

# Broker objects the harness provisions (see broker.py).
VPN = "default"
CLIENT_USERNAME = "feldera_user"
CLIENT_PASSWORD = "changeme"
INGEST_QUEUE = "feldera_perf_q"
ECHO_QUEUE = "feldera_perf_echo_q"
ECHO_TOPIC = "feldera/perf/echo"

PIPELINE_NAME = "solace_perf"


@dataclass
class RunConfig:
    """All knobs for one performance run.

    Field defaults mirror the CLI defaults in ``__main__.py``.
    """

    rate: int = 500
    duration_s: int = 60
    payload_bytes: int = 0
    publishers: int = 1
    window_size: int = 255
    delivery_mode: str = "direct"  # sink delivery: direct | persistent
    with_sink: bool = True
    preload: bool = False
    pipeline_workers: int = 4
    poll_interval_s: float = 1.0
    feldera_url: str = "http://localhost:8080"
    semp_url: str = "http://localhost:8088"
    smf_host: str = "localhost"
    smf_port: int = 55555
    # Hostname of the broker as seen from inside the compose network (what
    # the Feldera connector config must use).
    broker_host_internal: str = "solace"
    label: str = "run"
    results_dir: str = "results"
    keep_pipeline: bool = False

    @property
    def total_messages(self) -> int:
        return self.rate * self.duration_s

    def params_dict(self) -> dict:
        """Parameters recorded in the results JSON."""
        return {
            "rate": self.rate,
            "duration_s": self.duration_s,
            "payload_bytes": self.payload_bytes,
            "publishers": self.publishers,
            "window_size": self.window_size,
            "delivery_mode": self.delivery_mode,
            "with_sink": self.with_sink,
            "preload": self.preload,
            "pipeline_workers": self.pipeline_workers,
            "poll_interval_s": self.poll_interval_s,
        }
