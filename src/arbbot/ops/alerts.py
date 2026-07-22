"""ntfy push alerts (email fallback is manual: subscribe the topic in the
ntfy app or via email bridge). Disabled when no topic is configured —
alert() is then a stderr log, never a crash."""

from __future__ import annotations

import sys

import httpx


class Alerter:
    def __init__(self, topic: str = "", server: str = "https://ntfy.sh",
                 cooldown_s: float = 1800.0):
        self.topic = topic
        self.server = server
        self.cooldown_s = cooldown_s
        self._last: dict[str, float] = {}

    def alert(self, message: str) -> bool:
        print(f"[ALERT] {message}", file=sys.stderr)
        if not self.topic:
            return False
        import time
        key = message[:40]  # dedup flapping alerts by prefix
        now = time.monotonic()
        if now - self._last.get(key, -1e9) < self.cooldown_s:
            return False
        self._last[key] = now
        try:
            r = httpx.post(
                f"{self.server}/{self.topic}",
                content=message.encode(),
                headers={"Title": "arbbot", "Priority": "high"},
                timeout=10,
            )
            return r.status_code < 300
        except httpx.HTTPError:
            return False  # alerting must never take the recorder down


def smoke_test(topic: str) -> bool:
    """Delivery smoke test (test mandate): send one test notification."""
    return Alerter(topic).alert("arbbot alert delivery smoke test — this is a drill")
