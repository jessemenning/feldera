"""Paced multi-connection publisher to the ingest queue.

Each publisher thread owns its own MessagingService (sessions are not shared,
for maximum throughput) and publishes persistent messages straight to the
queue via the ``#P2P/QUE/<queue>`` topic, exactly like the materialize-solace
reference harness.

Pacing uses absolute deadlines (``target = start + k * interval``) so sleep
jitter never accumulates; if a thread falls behind it bursts to catch up and
records the worst deficit.
"""

import json
import logging
import threading
import time
from dataclasses import dataclass, field

from solace.messaging.messaging_service import MessagingService
from solace.messaging.resources.topic import Topic

from .config import CLIENT_PASSWORD, CLIENT_USERNAME, INGEST_QUEUE, VPN, RunConfig

log = logging.getLogger(__name__)


@dataclass
class PublisherStats:
    """Aggregated, thread-safe publish counters."""

    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    attempted: int = 0
    published: int = 0
    errors: int = 0
    max_pacing_deficit_ms: float = 0.0
    start_monotonic: float = 0.0
    end_monotonic: float = 0.0

    @property
    def duration_s(self) -> float:
        if self.end_monotonic <= self.start_monotonic:
            return 0.0
        return self.end_monotonic - self.start_monotonic

    @property
    def achieved_rate(self) -> float:
        d = self.duration_s
        return self.published / d if d > 0 else 0.0


def _build_payload(seq: int, pad: str) -> bytearray:
    # The message builder accepts a bytearray or str, not bytes; a bytearray
    # produces the binary attachment the Feldera connector reads via
    # get_payload() (a str would create an empty-attachment SDT text message).
    msg = {"seq": seq, "send_ts_ms": int(time.time() * 1000)}
    if pad:
        msg["pad"] = pad
    return bytearray(json.dumps(msg).encode())


def _compute_pad(payload_bytes: int) -> str:
    """Padding string sized so the serialized message ~= payload_bytes."""
    if payload_bytes <= 0:
        return ""
    # Overhead of {"seq": <10 digits>, "send_ts_ms": <13 digits>, "pad": ""}.
    overhead = len(_build_payload(9_999_999_999, "x")) - 1
    return "x" * max(1, payload_bytes - overhead)


class Publisher:
    """Runs cfg.publishers daemon threads; join() waits for completion."""

    def __init__(self, cfg: RunConfig):
        self.cfg = cfg
        self.stats = PublisherStats()
        self._threads: list[threading.Thread] = []

    def start(self) -> None:
        self.stats.start_monotonic = time.monotonic()
        n = self.cfg.publishers
        for i in range(n):
            t = threading.Thread(
                target=self._publish_thread, args=(i, n), daemon=True, name=f"pub-{i}"
            )
            t.start()
            self._threads.append(t)

    def is_alive(self) -> bool:
        return any(t.is_alive() for t in self._threads)

    def join(self, timeout_s: float | None = None) -> None:
        deadline = None if timeout_s is None else time.monotonic() + timeout_s
        for t in self._threads:
            remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
            t.join(remaining)
        with self.stats.lock:
            self.stats.end_monotonic = time.monotonic()

    def _connect(self) -> tuple:
        service = (
            MessagingService.builder()
            .from_properties(
                {
                    "solace.messaging.transport.host": f"tcp://{self.cfg.smf_host}:{self.cfg.smf_port}",
                    "solace.messaging.service.vpn-name": VPN,
                    "solace.messaging.authentication.scheme.basic.username": CLIENT_USERNAME,
                    "solace.messaging.authentication.scheme.basic.password": CLIENT_PASSWORD,
                }
            )
            .build()
        )
        service.connect()
        publisher = service.create_persistent_message_publisher_builder().build()
        publisher.start()
        return service, publisher

    def _publish_thread(self, index: int, n_threads: int) -> None:
        """Thread ``index`` publishes seq in {index, index+N, index+2N, ...}."""
        cfg = self.cfg
        try:
            service, publisher = self._connect()
        except Exception as e:
            log.error("publisher %d failed to connect: %s", index, e)
            with self.stats.lock:
                self.stats.errors += 1
            return

        dest = Topic.of(f"#P2P/QUE/{INGEST_QUEUE}")
        builder = service.message_builder()
        pad = _compute_pad(cfg.payload_bytes)
        total = cfg.total_messages
        interval = n_threads / cfg.rate  # seconds between this thread's sends
        start = time.monotonic()

        published = errors = 0
        max_deficit = 0.0
        local_k = 0
        for seq in range(index, total, n_threads):
            target = start + local_k * interval
            delta = target - time.monotonic()
            if delta > 0:
                time.sleep(delta)
            else:
                max_deficit = max(max_deficit, -delta * 1000)
            try:
                msg = builder.build(_build_payload(seq, pad))
                publisher.publish(message=msg, destination=dest)
                published += 1
            except Exception as e:
                errors += 1
                if errors <= 3:
                    log.error("publisher %d publish error: %s", index, e)
            local_k += 1

        try:
            publisher.terminate()
            service.disconnect()
        except Exception:
            pass

        with self.stats.lock:
            self.stats.attempted += local_k
            self.stats.published += published
            self.stats.errors += errors
            self.stats.max_pacing_deficit_ms = max(
                self.stats.max_pacing_deficit_ms, max_deficit
            )
