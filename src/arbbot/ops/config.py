"""Config + credential loading.

Credentials NEVER live in the repo, env vars, or config files. The trader
unit receives them via systemd LoadCredential ($CREDENTIALS_DIRECTORY);
the recorder unit gets no credentials at all (design: credential-free
recorder). For ad-hoc local runs, --credentials-dir points at a 0700 dir.
"""

from __future__ import annotations

import os
from decimal import Decimal
from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel, Field

from arbbot.fees.curves import FeeSchedule, PolymarketVariant


class RecorderConfig(BaseModel):
    registry_path: str = "config/registry.yaml"
    data_dir: str = "data/raw"
    scan_dir: str = "data/scan"
    health_path: str = "data/health.jsonl"
    socket_path: str = "data/arbbot.sock"
    kalshi_poll_interval_s: float = 5.0
    ntfy_topic: str = ""  # empty = alerts disabled
    min_edge_per_contract: Decimal = Decimal("0")
    polymarket_mode: str = "intl"  # "intl" (Geoff's account) | "us"
    # Polymarket US tag_slugs to record (credential-free REST poll). Empty =
    # PM US feed off. e.g. ["fed-decision", "cpi"].
    polymarket_us_tags: list[str] = Field(default_factory=list)

    def fee_schedule(self) -> FeeSchedule:
        return FeeSchedule(polymarket=PolymarketVariant(mode=self.polymarket_mode))


class ExecConfig(BaseModel):
    """Execution capital settings (config/exec.yaml, tracked in git)."""
    bankroll_usd: Decimal = Decimal("980")
    # fraction of bankroll one relationship class may tie up — keep in sync
    # with RiskConfig.per_class_cap (arbbot/risk/manager.py)
    per_class_cap: Decimal = Decimal("0.35")


def load_exec_config(path: str | Path = "config/exec.yaml") -> ExecConfig:
    p = Path(path)
    return ExecConfig() if not p.exists() else ExecConfig.model_validate(
        yaml.safe_load(p.read_text()) or {})


def load_recorder_config(path: str | Path = "config/recorder.yaml") -> RecorderConfig:
    p = Path(path)
    cfg = RecorderConfig() if not p.exists() else RecorderConfig.model_validate(
        yaml.safe_load(p.read_text()) or {})
    # the ntfy topic is a capability secret (knowing it = read/spam our alerts),
    # so it lives with the credentials, never in the (public) config file.
    if not cfg.ntfy_topic:
        cred = load_credential("ntfy_topic")
        if cred:
            cfg.ntfy_topic = cred.decode().strip()
        else:
            f = Path.home() / ".arbbot-credentials" / "ntfy_topic"
            if f.exists():
                cfg.ntfy_topic = f.read_text().strip()
    return cfg


def credentials_dir() -> Optional[Path]:
    """systemd LoadCredential dir, or ARBBOT_CREDENTIALS_DIR for local runs."""
    for var in ("CREDENTIALS_DIRECTORY", "ARBBOT_CREDENTIALS_DIR"):
        v = os.environ.get(var)
        if v:
            return Path(v)
    return None


def load_credential(name: str) -> Optional[bytes]:
    d = credentials_dir()
    if d is None:
        return None
    p = d / name
    return p.read_bytes() if p.exists() else None
