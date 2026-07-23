"""Runner-level venue quirks (card 7fab301e) — drives the REAL
arbbot.exec.main.run() coroutine with fake gateways/quoters, a fake private-WS
module, and a local unix-socket 'recorder'. Nothing touches a venue.

- pmus-private-order-ws: a private-WS EXECUTION_TYPE_FILL hedges immediately;
  when the 2s poll fallback later reports the SAME cumQuantity, the fill is
  hedged EXACTLY once (idempotence keyed on cumQuantity via hedged_by_oid).
- xv-429-at-rehearsal-proves-auth: a 429-throttled rehearsal proves the signed
  order path (authenticated, understood, throttled) — the runner proceeds;
  a non-429 rehearsal failure still ABORTS.
- pmus-unfilled-counts-against-cap: a take-take PM leg that reports unfilled
  still increments the fired/cap ratchet and blocks the next fire (naked PM
  shorts must not become invisible to the cap).
"""

import asyncio
import json
import sys
import time as _time
import types
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import httpx
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

import arbbot.exec.main as runner
import arbbot.exec.polymarket_us_gateway as pmus_gw_mod
import arbbot.record.polymarket_us as pmus_rec_mod
from arbbot.models.core import BookSnapshot, Level, Venue

KEY_PEM = rsa.generate_private_key(public_exponent=65537, key_size=2048).private_bytes(
    serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
    serialization.NoEncryption())
PM_SEED_B64 = b"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="

PMUS_PAIR_YAML = """\
relationships:
- id: mkr-pair
  type: equivalent-pair
  legs:
  - {venue: polymarket_us, market_id: mkA, side: 'yes', role: taker}
  - {venue: polymarket_us, market_id: mkB, side: 'yes', role: taker}
  verdict: equivalent
  vetted_by: human
"""
KALSHI_PAIR_YAML = """\
relationships:
- id: k-pair
  type: equivalent-pair
  legs:
  - {venue: kalshi, market_id: KXA, side: 'yes', role: taker}
  - {venue: kalshi, market_id: KXB, side: 'yes', role: taker}
  verdict: equivalent
  vetted_by: human
"""
TT_PAIR_YAML = """\
relationships:
- id: xvus-fedcut-26-test
  type: cross-venue-equivalent
  legs:
  - {venue: kalshi, market_id: KXFED, side: 'yes', role: taker}
  - {venue: polymarket_us, market_id: fed-slug, side: 'yes', role: taker}
  verdict: equivalent
  vetted_by: human
"""


class FakeQuoter:
    instances: list = []

    def __init__(self, rel=None, gateways=None, risk=None, markets=None, **kw):
        self.rel, self.gateways, self.risk = rel, gateways, risk
        self._resting: dict = {}
        self._last_quote_ts: dict = {}
        self.intents: list = []
        self.suppress: set = set()
        self.hedge_calls: list = []
        FakeQuoter.instances.append(self)

    def hedge_fill(self, i, side, books, qty):
        self.hedge_calls.append((i, side, qty))
        return {"hedged": True, "px": "0.28"}

    def on_book(self, books):
        pass

    def overdue_alarms(self, now):
        return []

    def cancel_all(self):
        pass


class _StubHttp:
    def get(self, url, **kw):
        return SimpleNamespace(json=lambda: {"positions": {}})


class FakePmusGateway:
    instances: list = []

    def __init__(self, kid, key, live=False):
        self.live = live
        self.base = "https://api.polymarket.us"
        self.client = _StubHttp()
        self.cum = 0                 # what the venue's order API reports
        self.fill_reads: list = []   # (order_id, reported cum)
        self.place_calls: list = []
        FakePmusGateway.instances.append(self)

    def _headers(self, method, path):
        return {}

    def filled_qty(self, oid):
        self.fill_reads.append((oid, self.cum))
        return self.cum

    def place_short(self, slug, price, count, post_only=False):
        self.place_calls.append((slug, str(price), count))
        return {"id": f"TT{len(self.place_calls)}"}


class FakeKalshiGateway:
    instances: list = []

    def __init__(self, kid, key, live=False):
        self.kid, self.key, self.live = kid, key, live
        self.place_calls: list = []
        FakeKalshiGateway.instances.append(self)

    def place_yes(self, ticker, side, price, count, post_only=True):
        self.place_calls.append((ticker, side, str(price), count))
        return {"order_id": f"K{len(self.place_calls)}"}

    def filled_qty(self, oid):
        return 0


class FakeKalshiCatalog:
    async def markets(self, tickers):
        return []


class FakePmusCatalog:
    async def markets_by_tags(self, tags):
        return []


def _fake_httpx_get(url, **kw):
    if "portfolio/balance" in url:
        payload = {"balance_dollars": "100"}
    elif "account/balances" in url:
        payload = {"balances": [{"buyingPower": "100"}]}
    elif "portfolio/positions" in url:
        payload = {"market_positions": []}
    else:
        payload = {}
    return SimpleNamespace(json=lambda: payload)


def patch_runner(monkeypatch, tmp_path, registry_yaml,
                 kalshi_cls=FakeKalshiGateway, pmus_cls=FakePmusGateway):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "registry.yaml").write_text(registry_yaml)
    cfg = SimpleNamespace(registry_path=str(tmp_path / "registry.yaml"),
                          socket_path=str(tmp_path / "arbbot.sock"),
                          ntfy_topic="", polymarket_us_tags=["t"],
                          scan_dir=str(tmp_path))
    creds = {"kalshi_api_key_id": b"kid", "kalshi_private_key.pem": KEY_PEM,
             "polymarket_usa_key_id": b"pmkid",
             "polymarket_usa_private_key": PM_SEED_B64}
    monkeypatch.setattr(runner, "load_recorder_config", lambda p: cfg)
    monkeypatch.setattr(runner, "load_credential", lambda n: creds.get(n))
    monkeypatch.setattr(runner, "Quoter", FakeQuoter)
    monkeypatch.setattr(runner, "KalshiOrderGateway", kalshi_cls)
    monkeypatch.setattr(runner, "KalshiCatalog", FakeKalshiCatalog)
    monkeypatch.setattr(pmus_gw_mod, "PolymarketUsOrderGateway", pmus_cls)
    monkeypatch.setattr(pmus_rec_mod, "PolymarketUsCatalog", FakePmusCatalog)
    monkeypatch.setattr(httpx, "get", _fake_httpx_get)
    FakeQuoter.instances.clear()
    FakePmusGateway.instances.clear()
    FakeKalshiGateway.instances.clear()
    return cfg


def _install_fake_ws(monkeypatch, frame_q):
    """sys.modules['websockets'] whose connect() yields frames from frame_q."""
    class _Ws:
        async def send(self, x):
            pass

        def __aiter__(self):
            return self

        async def __anext__(self):
            return await frame_q.get()

    class _Conn:
        def __call__(self, url, **kw):
            return self

        async def __aenter__(self):
            return _Ws()

        async def __aexit__(self, *a):
            return False

    mod = types.ModuleType("websockets")
    mod.connect = _Conn()
    monkeypatch.setitem(sys.modules, "websockets", mod)


def _fill_frame(oid, cum):
    return json.dumps({"orderSubscriptionUpdate": {"execution": {
        "type": "EXECUTION_TYPE_FILL",
        "order": {"id": oid, "cumQuantity": str(cum)}}}})


async def _wait_for(cond, timeout=8.0, msg="condition"):
    deadline = _time.monotonic() + timeout
    while _time.monotonic() < deadline:
        if cond():
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"timed out waiting for {msg}")


class _FastAsyncio:
    """Delegates to asyncio but caps sleeps — 60s rehearsal backoffs and 2s
    reconnect naps run in milliseconds while wait_for timeouts stay real."""

    def __getattr__(self, name):
        return getattr(asyncio, name)

    @staticmethod
    async def sleep(s):
        await asyncio.sleep(min(s, 0.01))


class _FastTime:
    def __getattr__(self, name):
        return getattr(_time, name)

    @staticmethod
    def sleep(s):
        _time.sleep(min(s, 0.001))


def snap(venue, mid, bid, ask, seq, size="100"):
    return (BookSnapshot(venue=venue, market_id=mid,
                         bids=[Level(price=Decimal(bid), size=Decimal(size))],
                         asks=[Level(price=Decimal(ask), size=Decimal(size))],
                         seq=seq, ts_local_ns=seq).model_dump_json() + "\n").encode()


# ------------------------------------------------ private-WS fill dedup ----

def test_ws_fill_hedged_once_when_poll_reports_same_cum(tmp_path, monkeypatch):
    cfg = patch_runner(monkeypatch, tmp_path, PMUS_PAIR_YAML)

    async def scenario():
        frame_q: asyncio.Queue = asyncio.Queue()
        _install_fake_ws(monkeypatch, frame_q)
        writers = []  # hold server-side writers alive (GC would EOF the client)
        server = await asyncio.start_unix_server(
            lambda r, w: writers.append(w), path=cfg.socket_path)
        task = asyncio.create_task(runner.run(["mkr-pair"], live=True, clip=5,
                                              config_path="x"))
        try:
            await _wait_for(lambda: FakeQuoter.instances, msg="quoter built")
            q = FakeQuoter.instances[-1]
            gw = FakePmusGateway.instances[-1]
            # a maker order resting on leg 0 (ask), clip 5
            q._resting[(0, "ask")] = SimpleNamespace(
                order_id="OID1", price=Decimal("0.30"), count=5)

            # 1) private WS pushes a partial fill cum=3 -> hedged immediately
            await frame_q.put(_fill_frame("OID1", 3))
            await _wait_for(lambda: q.hedge_calls, msg="WS-driven hedge")
            assert q.hedge_calls == [(0, "ask", 3)]

            # 2) the 2s poll fallback later reports the SAME cum=3 — it must
            # see the increment already hedged and hedge nothing
            gw.cum = 3
            await _wait_for(lambda: any(c == 3 for _, c in gw.fill_reads),
                            msg="poll observed cum=3")
            await asyncio.sleep(2.5)  # give further poll cycles a chance to double-hedge
            assert q.hedge_calls == [(0, "ask", 3)], \
                "poll re-reporting the same cumQuantity must NOT re-hedge"

            # 3) WS completes the fill (cum=5=count): hedge the NEW 2 only,
            # then stop resting so on_book requotes fresh
            await frame_q.put(_fill_frame("OID1", 5))
            await _wait_for(lambda: len(q.hedge_calls) == 2, msg="increment hedge")
            assert q.hedge_calls == [(0, "ask", 3), (0, "ask", 2)]
            await _wait_for(lambda: (0, "ask") not in q._resting,
                            msg="fully-filled quote popped")

            # each hedged increment recorded exactly once in the ledger
            recs = [json.loads(l) for l in
                    Path("data/exec/trades.jsonl").read_text().splitlines()]
            assert [r["qty"] for r in recs] == [3, 2]
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            for w in writers:
                w.close()
            server.close()

    asyncio.run(scenario())


# ------------------------------------------- rehearsal 429 proves auth ----

def test_rehearsal_429_proves_auth_and_proceeds(tmp_path, monkeypatch, capsys):
    class Kalshi429(FakeKalshiGateway):
        def rehearse(self, ticker):
            raise RuntimeError("429 Too Many Requests")

    patch_runner(monkeypatch, tmp_path, KALSHI_PAIR_YAML, kalshi_cls=Kalshi429)
    monkeypatch.setattr(runner, "asyncio", _FastAsyncio())

    async def scenario():
        task = asyncio.create_task(runner.run(["k-pair"], live=True, clip=5,
                                              config_path="x"))
        await asyncio.sleep(0.8)  # 3 fast-backoff attempts + into socket_loop
        assert not task.done(), "429-throttled rehearsal must not abort the trader"
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    asyncio.run(scenario())
    out = capsys.readouterr().out
    assert "auth verified by the 429 itself" in out
    assert "ABORTING" not in out


def test_rehearsal_non_429_failure_aborts(tmp_path, monkeypatch, capsys):
    class KalshiBroken(FakeKalshiGateway):
        def rehearse(self, ticker):
            raise ValueError("boom")

    patch_runner(monkeypatch, tmp_path, KALSHI_PAIR_YAML, kalshi_cls=KalshiBroken)
    monkeypatch.setattr(runner, "asyncio", _FastAsyncio())

    async def scenario():
        await asyncio.wait_for(runner.run(["k-pair"], live=True, clip=5,
                                          config_path="x"), timeout=10)

    asyncio.run(scenario())  # returns (aborts) instead of trading
    assert "ABORTING" in capsys.readouterr().out


# ------------------------------------- unfilled PM leg counts against cap ----

def test_take_take_unfilled_pm_leg_still_counts_against_cap(tmp_path, monkeypatch, capsys):
    cfg = patch_runner(monkeypatch, tmp_path, TT_PAIR_YAML)
    monkeypatch.setattr(runner, "TT_COOLDOWN", 0.0)   # isolate the cap ratchet
    monkeypatch.setattr(runner, "time", _FastTime())  # fast unfilled-poll waits

    async def scenario():
        writers = []
        server = await asyncio.start_unix_server(
            lambda r, w: writers.append(w), path=cfg.socket_path)
        task = asyncio.create_task(runner.run(["xvus-fedcut-26-test"], live=True,
                                              clip=5, config_path="x"))
        try:
            await _wait_for(lambda: writers, msg="trader connected")
            gw = FakePmusGateway.instances[-1]
            kgw = FakeKalshiGateway.instances[-1]
            w = writers[0]
            # crossed books: Kalshi ask 0.40 vs PM bid 0.50, deep enough that
            # the attempted size == TT_CAP (50)
            w.write(snap(Venue.KALSHI, "KXFED", "0.30", "0.40", seq=1))
            w.write(snap(Venue.POLYMARKET_US, "fed-slug", "0.50", "0.60", seq=1))
            await w.drain()
            # PM leg fires but reports UNFILLED (gw.cum stays 0) -> abort,
            # never place the Kalshi leg, count attempted size against cap
            await _wait_for(lambda: gw.place_calls, msg="take-take PM leg fired")
            assert gw.place_calls == [("fed-slug", "0.50", 50)]
            await _wait_for(
                lambda: len(gw.fill_reads) >= 6, msg="unfilled polls done")
            assert kgw.place_calls == [], "unfilled PM leg must not hedge Kalshi"

            # books still crossed, cooldown disabled: ONLY the fired-cap
            # ratchet can block the refire
            w.write(snap(Venue.KALSHI, "KXFED", "0.30", "0.40", seq=2))
            w.write(snap(Venue.POLYMARKET_US, "fed-slug", "0.50", "0.60", seq=2))
            await w.drain()
            await asyncio.sleep(1.0)
            assert gw.place_calls == [("fed-slug", "0.50", 50)], \
                "attempted-but-unfilled size must block the next fire (cap ratchet)"
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            for w in writers:
                w.close()
            server.close()

    asyncio.run(scenario())
    out = capsys.readouterr().out
    assert "counting attempted size against cap" in out
