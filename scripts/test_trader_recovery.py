#!/usr/bin/env python3
"""Exercise the trader restart policy on isolated, order-free systemd units.

Requires a host user systemd session (systemd >= 254). Production units,
credentials, and trading binaries are never used. Retry durations are scaled
for the test; all other recovery settings come from the checked-in unit.
"""
import json
from pathlib import Path
import subprocess
import tempfile
import time
import unittest
import uuid

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = '''import json, pathlib, sys, time
path = pathlib.Path(sys.argv[1])
mode = sys.argv[2]
count = len(path.read_text().splitlines()) if path.exists() else 0
with path.open("a") as stream:
    stream.write(json.dumps({"attempt": count + 1, "time": time.monotonic()}) + "\\n")
if mode == "config":
    sys.exit(2)
if mode == "forever" or count < 6:
    sys.exit(10)
time.sleep(120)
'''


class RecoveryTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="arbbot-recovery-proof-")
        self.directory = Path(self.temp.name)
        self.fixture = self.directory / "fixture.py"
        self.fixture.write_text(FIXTURE)
        self.units = []
        # Ignore ExecStart's continuation lines: only recovery directives matter.
        self.policy = {}
        for line in (ROOT / "systemd/arbbot-trader-m3.service").read_text().splitlines():
            if line.startswith(("Restart=", "RestartSec=", "RestartSteps=",
                                "RestartMaxDelaySec=", "RestartPreventExitStatus=",
                                "StartLimitIntervalSec=", "StartLimitBurst=")):
                key, value = line.split("=", 1)
                self.policy[key] = value

    def tearDown(self):
        for unit in self.units:
            subprocess.run(["systemctl", "--user", "stop", unit], capture_output=True)
            subprocess.run(["systemctl", "--user", "reset-failed", unit], capture_output=True)
        self.temp.cleanup()

    def start(self, mode):
        unit = "arbbot-recovery-proof-" + uuid.uuid4().hex + ".service"
        self.units.append(unit)
        log = self.directory / (mode + ".jsonl")
        policy = dict(self.policy)
        # Same 1:10 backoff ratio and number of steps, at test timescale.
        base = float(policy["RestartSec"])
        cap = float(policy["RestartMaxDelaySec"])
        policy["RestartSec"] = "0.1s"
        policy["RestartMaxDelaySec"] = f"{0.1 * cap / base}s"
        cmd = ["systemd-run", "--user", "--quiet", "--unit", unit,
               "--property=Type=exec"]
        cmd.extend(f"--property={key}={value}" for key, value in policy.items())
        cmd.extend(["/usr/bin/python3", str(self.fixture), str(log), mode])
        subprocess.run(cmd, check=True, capture_output=True, text=True)
        return unit, log

    def state(self, unit):
        result = subprocess.run(["systemctl", "--user", "show", unit,
                                 "-p", "SubState", "--value"],
                                check=True, capture_output=True, text=True)
        return result.stdout.strip()

    def rows(self, log):
        return [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []

    def wait_for(self, predicate):
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.05)
        self.fail("recovery state not reached within 20 seconds")

    def stop_and_prove_stopped(self, unit, log):
        subprocess.run(["systemctl", "--user", "stop", unit], check=True, capture_output=True)
        count = len(self.rows(log))
        time.sleep(1.3)  # beyond the longest scaled restart delay
        self.assertEqual(len(self.rows(log)), count, "manual stop must cancel pending recovery")
        self.assertEqual(self.state(unit), "dead")

    def test_outage_exceeds_old_budget_then_recovers_and_stays_stopped(self):
        unit, log = self.start("recover")
        self.wait_for(lambda: len(self.rows(log)) >= 7 and self.state(unit) == "running")
        times = [row["time"] for row in self.rows(log)]
        gaps = [b - a for a, b in zip(times, times[1:])]
        self.assertGreaterEqual(gaps[-1], 0.9)
        self.assertGreaterEqual(gaps[-2], 0.9)
        self.assertGreater(gaps[-1], gaps[0] * 2, "restart delay must grow")
        self.stop_and_prove_stopped(unit, log)

    def test_manual_stop_during_outage_cancels_the_retry(self):
        unit, log = self.start("forever")
        self.wait_for(lambda: len(self.rows(log)) >= 5 and self.state(unit) == "auto-restart")
        self.stop_and_prove_stopped(unit, log)

    def test_invalid_arguments_fail_without_retrying(self):
        unit, log = self.start("config")
        self.wait_for(lambda: self.state(unit) == "failed")
        time.sleep(0.3)
        self.assertEqual(len(self.rows(log)), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
