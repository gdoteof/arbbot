Trader outage recovery

`arbbot-trader-m3` automatically retries unsuccessful runs. Delays grow from
30 seconds to approximately 53, 95, 169, and then 300 seconds. The five-minute
cap limits retry traffic, not the number of attempts. There is no exhausted
start budget that requires a person to reset it after a long venue outage.

Each retry launches a fresh trader and runs the existing cancellation and
verification sweep before trading can begin. A responsive health endpoint is
not sufficient: the actual authenticated order APIs must permit verification.
Recovery therefore occurs on a successful retry, up to five minutes plus
startup time after the required venue APIs recover. Other startup prerequisites
must also pass; neither uncertain orders nor a KILL file are bypassed.

`systemctl --user stop arbbot-trader-m3` remains an intentional stop and cancels
pending automatic retries. Invalid command-line arguments (exit 2) stop retrying
and require a configuration fix. Other nonzero exits, including startup sweep
failure (10) and uncertain shutdown cleanup (17/18), retry with backoff.
The service remains enabled for startup at login/boot under the existing user
manager configuration. Backoff and NRestarts are cumulative until the service's
restart state is reset; a later failure after recovery can therefore retain the
five-minute delay.

The existing watchdog distinguishes terminal FAILED from RECOVERING. During an
auto-restart wait after at least three cumulative restarts, it reports recovery
using its existing notification cooldown. Intentional stops remain quiet. This
monitoring does not alter recovery or clear a trading safety check.

Inspect the current state:

```bash
systemctl --user show arbbot-trader-m3 \
  -p ActiveState -p SubState -p NRestarts -p RestartUSecNext
journalctl --user -u arbbot-trader-m3 -n 40 --no-pager
```

Policy lives in `systemd/arbbot-trader-m3.service`; the installed user unit has
the same contents. The existing `arm.conf` overrides only the trading command
and was preserved. This uses systemd >=254; the deployment runs systemd 257.
Directive definitions: [systemd's service documentation](https://github.com/systemd/systemd/blob/v257/man/systemd.service.xml).

Validation:

- `systemd-analyze --user verify systemd/arbbot-trader-m3.service`
- `bash scripts/test_freshness_gauges.sh`: 77 checks passed, with notification
  delivery stubbed; no test sends real notifications.
- `python3 scripts/test_trader_recovery.py`: three host integration tests on
  isolated transient services. Six simulated outage failures recover on the
  seventh start, retry delays grow and cap, manual stop cancels recovery both
  during outage and after recovery, and invalid arguments do not restart.
  The test uses dummy processes and never loads credentials or places orders.

Deployment on 2026-09-18: policy installed and reloaded, prior failure counter
cleared, and the service started. PM-US still returned HTTP 503; systemd then
reported `activating / auto-restart`, `RestartMaxDelayUSec=5min`, and
`StartLimitIntervalUSec=0`. Trading is waiting for successful startup verification.
