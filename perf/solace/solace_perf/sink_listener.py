"""Echo-queue consumer: true per-message end-to-end latency.

A daemon thread binds to the echo verification queue (fed by the pipeline's
``solace_output`` sink) and records ``recv_ts - send_ts`` per message. The
publisher and this listener run in the same process on the same host clock,
so the difference is skew-free.

Only the first receipt of each ``seq`` is counted; redeliveries (possible
under at-least-once) increment ``duplicate_count`` instead.
"""

import json
import logging
import threading
import time
from array import array

from solace.messaging.messaging_service import MessagingService
from solace.messaging.receiver.message_receiver import MessageHandler
from solace.messaging.resources.queue import Queue

from .config import CLIENT_PASSWORD, CLIENT_USERNAME, ECHO_QUEUE, VPN, RunConfig

log = logging.getLogger(__name__)

_UNSET = -1


class SinkListener(MessageHandler):
    """Consumes the echo queue and accumulates per-seq latency samples."""

    def __init__(self, cfg: RunConfig):
        self.cfg = cfg
        # Seq-indexed latency in ms; 8 B * 3 M msgs = 24 MB at the largest
        # suggested run. _UNSET marks "not yet received".
        self._latency_ms = array("q", [_UNSET] * cfg.total_messages)
        self._lock = threading.Lock()
        self.received_count = 0
        self.duplicate_count = 0
        self.decode_errors = 0
        self._service = None
        self._receiver = None

    def start(self) -> None:
        self._service = (
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
        self._service.connect()
        self._receiver = (
            self._service.create_persistent_message_receiver_builder().build(
                Queue.durable_exclusive_queue(ECHO_QUEUE)
            )
        )
        self._receiver.start()
        self._receiver.receive_async(self)

    def on_message(self, message) -> None:
        recv_ts_ms = int(time.time() * 1000)
        try:
            payload = message.get_payload_as_string()
            if payload is None:
                payload = bytes(message.get_payload_as_bytes()).decode()
            row = json.loads(payload)
            # The JSON encoder wraps rows as {"insert": {...}}.
            row = row.get("insert", row)
            seq = int(row["seq"])
            send_ts_ms = int(row["send_ts_ms"])
        except Exception:
            with self._lock:
                self.decode_errors += 1
            self._ack(message)
            return

        with self._lock:
            if 0 <= seq < len(self._latency_ms):
                if self._latency_ms[seq] == _UNSET:
                    self._latency_ms[seq] = recv_ts_ms - send_ts_ms
                    self.received_count += 1
                else:
                    self.duplicate_count += 1
            else:
                self.decode_errors += 1
        self._ack(message)

    def _ack(self, message) -> None:
        try:
            self._receiver.ack(message)
        except Exception:
            pass

    def latencies_ms(self) -> list[int]:
        """All recorded latency samples (unordered)."""
        with self._lock:
            return [v for v in self._latency_ms if v != _UNSET]

    def seq_gaps(self, expected_total: int) -> int:
        """Number of sequence numbers never received."""
        with self._lock:
            return sum(
                1 for v in self._latency_ms[:expected_total] if v == _UNSET
            )

    def stop(self) -> None:
        for closer in (
            lambda: self._receiver and self._receiver.terminate(),
            lambda: self._service and self._service.disconnect(),
        ):
            try:
                closer()
            except Exception:
                pass
