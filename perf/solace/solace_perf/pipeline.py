"""Feldera pipeline setup: SQL generation and lifecycle management."""

import json
import logging

from feldera import PipelineBuilder
from feldera.enums import CompilationProfile
from feldera.rest.feldera_client import FelderaClient
from feldera.runtime_config import RuntimeConfig

from .config import (
    CLIENT_PASSWORD,
    CLIENT_USERNAME,
    ECHO_TOPIC,
    INGEST_QUEUE,
    PIPELINE_NAME,
    VPN,
    RunConfig,
)

log = logging.getLogger(__name__)


def _input_connector(cfg: RunConfig) -> str:
    """Connector JSON for the ingest table, embedded in the SQL WITH clause."""
    connector = [
        {
            "name": "solace_in",
            "transport": {
                "name": "solace_input",
                "config": {
                    "host": cfg.broker_host_internal,
                    "port": 55555,
                    "vpn": VPN,
                    "username": CLIENT_USERNAME,
                    "password": CLIENT_PASSWORD,
                    "queue": INGEST_QUEUE,
                    "window_size": cfg.window_size,
                },
            },
            "format": {
                "name": "json",
                "config": {"update_format": "raw", "array": False},
            },
        }
    ]
    return json.dumps(connector)


def _sink_connector(cfg: RunConfig) -> str:
    """Connector JSON for the echo view's Solace Platform output."""
    connector = [
        {
            "name": "solace_echo",
            "transport": {
                "name": "solace_output",
                "config": {
                    "host": cfg.broker_host_internal,
                    "port": 55555,
                    "vpn": VPN,
                    "username": CLIENT_USERNAME,
                    "password": CLIENT_PASSWORD,
                    "topic": ECHO_TOPIC,
                    "delivery_mode": cfg.delivery_mode,
                },
            },
            "format": {
                "name": "json",
                "config": {
                    "update_format": "insert_delete",
                    # `events` is append-only so retractions never occur;
                    # skip_deletes is belt-and-braces.
                    "skip_deletes": True,
                },
            },
        }
    ]
    return json.dumps(connector)


def build_sql(cfg: RunConfig) -> str:
    """Generate the perf pipeline SQL.

    The view cascade deliberately mirrors the materialize-solace reference
    (mv1 projection -> mv2 per-second aggregate -> mv3 global rollup) so the
    circuit does non-trivial incremental work per message. All views are
    materialized so ad-hoc SELECTs work against them.
    """
    # SQL string literals double any single quotes; connector JSON has none.
    sql = f"""
CREATE TABLE events (
    seq BIGINT NOT NULL,
    send_ts_ms BIGINT NOT NULL,
    pad VARCHAR
) WITH (
    'materialized' = 'true',
    'connectors' = '{_input_connector(cfg)}'
);

-- mv1: row-level projection (parse/flatten stage).
CREATE MATERIALIZED VIEW mv1 AS
    SELECT seq, send_ts_ms, CHAR_LENGTH(COALESCE(pad, '')) AS pad_len
    FROM events;

-- mv2: per-second aggregate keyed on the publisher clock.
CREATE MATERIALIZED VIEW mv2 AS
    SELECT send_ts_ms / 1000    AS sec,
           COUNT(*)             AS msg_count,
           MAX(seq)             AS max_seq,
           MAX(send_ts_ms)      AS max_send_ts_ms
    FROM mv1
    GROUP BY send_ts_ms / 1000;

-- mv3: global rollup -- the "deepest view" polled for visible lag.
CREATE MATERIALIZED VIEW mv3 AS
    SELECT SUM(msg_count)       AS total_msgs,
           COUNT(*)             AS seconds_with_data,
           MAX(max_seq)         AS latest_seq,
           MAX(max_send_ts_ms)  AS latest_send_ts_ms
    FROM mv2;
"""
    if cfg.with_sink:
        sql += f"""
-- echo: per-row timestamps carried through to the Solace Platform sink,
-- consumed by the harness for true per-message end-to-end latency.
CREATE MATERIALIZED VIEW echo WITH (
    'connectors' = '{_sink_connector(cfg)}'
) AS
    SELECT seq, send_ts_ms FROM mv1;
"""
    return sql


class PerfPipeline:
    """Owns the Feldera pipeline for one run."""

    def __init__(self, cfg: RunConfig):
        self.cfg = cfg
        self.client = FelderaClient(cfg.feldera_url)
        self.pipeline = None

    def create(self) -> None:
        sql = build_sql(self.cfg)
        log.info("Creating pipeline %r (compilation may take minutes)", PIPELINE_NAME)
        self.pipeline = PipelineBuilder(
            self.client,
            name=PIPELINE_NAME,
            sql=sql,
            compilation_profile=CompilationProfile.OPTIMIZED,
            runtime_config=RuntimeConfig(workers=self.cfg.pipeline_workers),
        ).create_or_replace()

    def start(self) -> None:
        self.pipeline.start()

    def start_paused(self) -> None:
        self.pipeline.start_paused()

    def resume(self) -> None:
        self.pipeline.resume()

    def global_metrics(self):
        return self.pipeline.stats().global_metrics

    def query_mv3(self) -> dict:
        """One-row rollup from the deepest view; {} when unavailable."""
        rows = list(
            self.pipeline.query(
                "SELECT total_msgs, latest_seq, latest_send_ts_ms FROM mv3"
            )
        )
        return rows[0] if rows else {}

    def teardown(self) -> None:
        if self.pipeline is None:
            return
        if self.cfg.keep_pipeline:
            log.info("--keep-pipeline: leaving pipeline running")
            return
        try:
            self.pipeline.stop(force=True)
        except Exception as e:  # teardown is best-effort
            log.warning("pipeline stop failed: %s", e)
