"""SEMP v2 helpers: provision broker objects and read queue statistics.

Provisioning is idempotent: HTTP 400/409 ("already exists") is treated as
success so repeated runs against a warm broker work.
"""

import logging
import time

import requests

from .config import (
    CLIENT_PASSWORD,
    CLIENT_USERNAME,
    ECHO_QUEUE,
    ECHO_TOPIC,
    INGEST_QUEUE,
    VPN,
)

log = logging.getLogger(__name__)

ADMIN_AUTH = ("admin", "admin")


class Semp:
    """Thin SEMP v2 client bound to one broker and message VPN."""

    def __init__(self, base_url: str, vpn: str = VPN):
        self.config_base = f"{base_url}/SEMP/v2/config/msgVpns/{vpn}"
        self.monitor_base = f"{base_url}/SEMP/v2/monitor/msgVpns/{vpn}"

    def post_config(self, path: str, body: dict) -> None:
        """POST a config object; tolerate 'already exists' responses."""
        resp = requests.post(
            f"{self.config_base}/{path}", json=body, auth=ADMIN_AUTH, timeout=10
        )
        if resp.status_code not in (200, 400, 409):
            raise RuntimeError(
                f"SEMP POST {path} failed: {resp.status_code} {resp.text}"
            )

    def delete_config(self, path: str) -> None:
        """DELETE a config object; tolerate 'does not exist' responses."""
        resp = requests.delete(
            f"{self.config_base}/{path}", auth=ADMIN_AUTH, timeout=10
        )
        if resp.status_code not in (200, 400, 404):
            raise RuntimeError(
                f"SEMP DELETE {path} failed: {resp.status_code} {resp.text}"
            )

    def provision(self, with_sink: bool) -> None:
        """Create the client username and perf queues for a run.

        Queues are deleted and recreated so that message-ID-based backlog
        arithmetic (see queue_stats) starts from a clean slate every run.
        """
        self.post_config(
            "clientUsernames",
            {
                "clientUsername": CLIENT_USERNAME,
                "password": CLIENT_PASSWORD,
                "enabled": True,
                "aclProfileName": "default",
                "clientProfileName": "default",
            },
        )
        self._recreate_queue(INGEST_QUEUE)
        if with_sink:
            self._recreate_queue(ECHO_QUEUE)
            # A queue topic subscription also promotes matching *direct*
            # messages to the spool, so the same listener covers both
            # delivery modes of the sink.
            self.post_config(
                f"queues/{ECHO_QUEUE}/subscriptions",
                {"subscriptionTopic": ECHO_TOPIC},
            )

    def _recreate_queue(self, queue: str) -> None:
        self.delete_config(f"queues/{queue}")
        self.post_config(
            "queues",
            {
                "queueName": queue,
                "accessType": "exclusive",
                "permission": "consume",
                "ingressEnabled": True,
                "egressEnabled": True,
            },
        )

    def queue_stats(self, queue: str) -> dict:
        """Return backlog statistics for a queue.

        ``q_backlog = lastSpooledMsgId - highestAckedMsgId`` is the only
        correct measure of undelivered/unacked depth: ``spooledMsgCount`` is
        cumulative and never decrements as messages are consumed, so a fully
        drained queue still reports the total ever spooled.

        Returns ``q_backlog = -1`` when the fields are unavailable (e.g. no
        message has been spooled yet).
        """
        resp = requests.get(
            f"{self.monitor_base}/queues/{queue}", auth=ADMIN_AUTH, timeout=10
        )
        if resp.status_code != 200:
            return {"q_backlog": -1, "spooled_total": -1, "tx_unacked": -1}
        data = resp.json().get("data", {})
        last_id = data.get("lastSpooledMsgId")
        acked_id = data.get("highestAckedMsgId")
        backlog = -1
        if last_id is not None and acked_id is not None:
            backlog = int(last_id) - int(acked_id)
        return {
            "q_backlog": backlog,
            "spooled_total": int(data.get("spooledMsgCount", -1)),
            "tx_unacked": int(data.get("txUnackedMsgCount", -1)),
        }

    def wait_for_backlog(
        self, queue: str, expected: int, timeout_s: float = 60.0
    ) -> int:
        """Poll until the queue backlog reaches ``expected`` (preload mode)."""
        deadline = time.monotonic() + timeout_s
        backlog = -1
        while time.monotonic() < deadline:
            backlog = self.queue_stats(queue)["q_backlog"]
            if backlog >= expected:
                return backlog
            time.sleep(1.0)
        log.warning(
            "queue %s backlog %d never reached %d within %.0fs",
            queue,
            backlog,
            expected,
            timeout_s,
        )
        return backlog
