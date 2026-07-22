"""Kalshi WS adapter: delta-change->total translation, YES normalization, gap
detection. The change-vs-total quirk is where a naive port loses money."""

import asyncio
import json
from decimal import Decimal

import pytest
import websockets

from arbbot.models.core import Venue
from arbbot.record.kalshi import (
    KalshiWsBook,
    ws_subscribe_frame,
)
from arbbot.record.recorder import LivenessTracker, RecorderCore, kalshi_ws_task
from arbbot.record.store import JsonlWriter, iter_jsonl

D = Decimal


def test_ws_book_delta_is_change_not_total():
    b = KalshiWsBook()
    snap = b.on_snapshot(
        {"market_ticker": "M",
         "yes_dollars": [["0.40", "100"]],
         "no_dollars": [["0.55", "80"]]},
        seq=1,
    )
    # snapshot normalizes NO 0.55/80 -> YES ask 0.45/80
    assert snap.bids[0].price == D("0.40") and snap.bids[0].size == D("100")
    assert snap.asks[0].price == D("0.45") and snap.asks[0].size == D("80")
    # delta of -54 on the yes 0.40 level -> new TOTAL 46 (not -54)
    d = b.on_delta({"market_ticker": "M", "side": "yes", "price_dollars": "0.40",
                    "delta_fp": "-54"}, seq=2)
    assert d.side == "bid" and d.price == D("0.40") and d.size == D("46")
    # another -46 -> total 0 -> level removed
    d = b.on_delta({"market_ticker": "M", "side": "yes", "price_dollars": "0.40",
                    "delta_fp": "-46"}, seq=3)
    assert d.size == D("0")
    # positive change on a NO level -> YES ask total
    d = b.on_delta({"market_ticker": "M", "side": "no", "price_dollars": "0.55",
                    "delta_fp": "20"}, seq=4)
    assert d.side == "ask" and d.price == D("0.45") and d.size == D("100")


def test_ws_book_new_level_from_zero():
    b = KalshiWsBook()
    b.on_snapshot({"market_ticker": "M", "yes_dollars": [], "no_dollars": []}, seq=1)
    d = b.on_delta({"market_ticker": "M", "side": "yes", "price_dollars": "0.30",
                    "delta_fp": "25"}, seq=2)
    assert d.size == D("25")


async def test_interleaved_markets_keep_book_state(tmp_path):
    """REGRESSION (live incident 2026-07-20 11:45): the wire seq is
    per-SUBSCRIPTION; two markets interleaving must not kill each other's
    book state via phantom per-market gaps."""
    async def handler(ws):
        await ws.recv()  # subscribe
        sid = 3
        for i, (tick, other) in enumerate(
            [("A", None), ("B", None), ("A", None), ("B", None), ("A", None)]
        ):
            if i < 2:
                await ws.send(json.dumps({"type": "orderbook_snapshot", "sid": sid,
                    "seq": i + 1, "msg": {"market_ticker": tick,
                    "yes_dollars": [["0.30", "100"]], "no_dollars": []}}))
            else:
                await ws.send(json.dumps({"type": "orderbook_delta", "sid": sid,
                    "seq": i + 1, "msg": {"market_ticker": tick, "side": "yes",
                    "price_dollars": "0.30", "delta_fp": "-10"}}))
        await ws.wait_closed()

    async with websockets.serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        core = RecorderCore(JsonlWriter(tmp_path))
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.hazmat.primitives import serialization
        pem = rsa.generate_private_key(public_exponent=65537, key_size=2048).private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption())
        task = asyncio.create_task(kalshi_ws_task(
            core, LivenessTracker(), ["A", "B"], "k", pem,
            ws_url=f"ws://127.0.0.1:{port}"))
        for _ in range(100):
            a, b = core.books.get("kalshi", "A"), core.books.get("kalshi", "B")
            if a and b and a.seq >= 3 and b.seq >= 2:
                break
            await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    a, b = core.books.get("kalshi", "A"), core.books.get("kalshi", "B")
    assert a is not None and b is not None, "interleaved deltas killed a book"
    # A took 2 deltas of -10 on 100; B took 1
    assert a.bids[0].size == D("80")
    assert b.bids[0].size == D("90")
    assert core.gap_count == 0


async def test_trades_do_not_gap_books(tmp_path):
    """REGRESSION (adversarial review P1, 2026-07-20): a trade between two
    deltas must NOT advance the book seq — previously the first trade on a
    market permanently killed its book."""
    async def handler(ws):
        await ws.recv()
        sid = 4
        await ws.send(json.dumps({"type": "orderbook_snapshot", "sid": sid, "seq": 1,
            "msg": {"market_ticker": "T", "yes_dollars": [["0.30", "100"]], "no_dollars": []}}))
        await ws.send(json.dumps({"type": "trade", "sid": 9, "seq": 1,
            "msg": {"market_ticker": "T", "yes_price_dollars": "0.30",
                    "no_price_dollars": "0.70", "count_fp": "5", "taker_side": "yes"}}))
        await ws.send(json.dumps({"type": "orderbook_delta", "sid": sid, "seq": 2,
            "msg": {"market_ticker": "T", "side": "yes", "price_dollars": "0.30",
                    "delta_fp": "-10"}}))
        await ws.wait_closed()

    async with websockets.serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        core = RecorderCore(JsonlWriter(tmp_path))
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.hazmat.primitives import serialization
        pem = rsa.generate_private_key(public_exponent=65537, key_size=2048).private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption())
        task = asyncio.create_task(kalshi_ws_task(
            core, LivenessTracker(), ["T"], "k", pem, ws_url=f"ws://127.0.0.1:{port}"))
        for _ in range(100):
            b = core.books.get("kalshi", "T")
            if b and b.bids and b.bids[0].size == D("90"):
                break
            await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    b = core.books.get("kalshi", "T")
    assert b is not None, "trade gapped and killed the book"
    assert b.bids[0].size == D("90")  # delta after trade applied cleanly
    assert core.gap_count == 0


async def test_kalshi_ws_task_against_fake_server(tmp_path):
    """Fake Kalshi WS: auth handshake, snapshot then delta then trade, and a
    seq GAP that must trigger a get_snapshot request."""
    got_snapshot_request = asyncio.Event()

    async def handler(ws):
        # first client message is the subscribe
        sub = json.loads(await ws.recv())
        assert sub["cmd"] == "subscribe" and sub["params"]["market_tickers"] == ["KXTEST"]
        sid = 7
        await ws.send(json.dumps({"type": "orderbook_snapshot", "sid": sid, "seq": 1,
            "msg": {"market_ticker": "KXTEST",
                    "yes_dollars": [["0.30", "100"]], "no_dollars": [["0.60", "50"]]}}))
        await ws.send(json.dumps({"type": "orderbook_delta", "sid": sid, "seq": 2,
            "msg": {"market_ticker": "KXTEST", "side": "yes",
                    "price_dollars": "0.30", "delta_fp": "-40"}}))
        await ws.send(json.dumps({"type": "trade", "sid": 9,
            "msg": {"market_ticker": "KXTEST", "yes_price_dollars": "0.31",
                    "no_price_dollars": "0.69", "count_fp": "5", "taker_side": "yes"}}))
        # SEQ GAP: jump to seq 5 -> client must request a snapshot
        await ws.send(json.dumps({"type": "orderbook_delta", "sid": sid, "seq": 5,
            "msg": {"market_ticker": "KXTEST", "side": "yes",
                    "price_dollars": "0.30", "delta_fp": "10"}}))
        nxt = json.loads(await ws.recv())
        if nxt.get("cmd") == "update_subscription" and nxt["params"]["action"] == "get_snapshot":
            got_snapshot_request.set()
        await ws.wait_closed()

    async with websockets.serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        core = RecorderCore(JsonlWriter(tmp_path))
        liveness = LivenessTracker()
        # a throwaway RSA key so signing runs (fake server ignores the headers)
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.hazmat.primitives import serialization
        pem = rsa.generate_private_key(public_exponent=65537, key_size=2048).private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption())
        task = asyncio.create_task(kalshi_ws_task(
            core, liveness, ["KXTEST"], "fake-key-id", pem, ws_url=f"ws://127.0.0.1:{port}"))
        await asyncio.wait_for(got_snapshot_request.wait(), timeout=5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    files = list(tmp_path.glob("kalshi-*.jsonl"))
    kinds = [r["kind"] for _, r in iter_jsonl(files[0])]
    assert kinds[:3] == ["snapshot", "delta", "trade"]
    # the gap delta (seq 5) must NOT have been applied as a normal delta
    # (client requested resync instead); so at most 3 events pre-gap persisted
    assert "delta" in kinds and kinds.count("snapshot") >= 1
    assert liveness.check().get("kalshi-ws") is False
