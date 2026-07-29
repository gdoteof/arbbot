"""Recorder core, liveness, broadcaster, and fake-WS integration harness."""

import asyncio
import json
from decimal import Decimal

import pytest
import websockets

from arbbot.models.core import BookDelta, BookSnapshot, Level, Trade, Venue
from arbbot.record.polymarket import ClobRest, SeqCounter
from arbbot.record.recorder import (
    LivenessTracker,
    RecorderCore,
    UnixBroadcaster,
    polymarket_ws_task,
)
from arbbot.record.store import JsonlWriter, iter_jsonl


def snap(mid="M", seq=1):
    return BookSnapshot(
        venue=Venue.KALSHI,
        market_id=mid,
        bids=[Level(price=Decimal("0.4"), size=Decimal("1"))],
        asks=[],
        seq=seq,
        ts_local_ns=seq,
    )


def delta(mid="M", seq=2):
    return BookDelta(
        venue=Venue.KALSHI, market_id=mid, side="bid",
        price=Decimal("0.4"), size=Decimal("2"), seq=seq, ts_local_ns=seq,
    )


def test_recorder_core_persists_applies_and_flags_resync(tmp_path):
    core = RecorderCore(JsonlWriter(tmp_path))
    assert core.on_event(snap()) is None
    assert core.on_event(delta(seq=2)) is None
    assert core.books.get("kalshi", "M").bids[0].size == Decimal("2")
    # gap -> resync request, event still persisted for forensics
    assert core.on_event(delta(seq=9)) == "M"
    assert core.gap_count == 1
    files = list(tmp_path.glob("kalshi-*.jsonl"))
    assert len(files) == 1 and len(list(iter_jsonl(files[0]))) == 3


def test_liveness_quiet_book_is_healthy_dead_conn_is_not():
    lt = LivenessTracker(stale_after_s=0.0)  # everything instantly stale
    lt.beat("conn-a")
    assert lt.check()["conn-a"] is True  # no beat since -> stale
    lt2 = LivenessTracker(stale_after_s=60.0)
    lt2.beat("conn-a")
    assert lt2.check()["conn-a"] is False  # recent heartbeat -> healthy


async def test_unix_broadcaster_fanout_and_slow_client_drop(tmp_path):
    b = UnixBroadcaster(tmp_path / "arb.sock")
    await b.start()
    reader, writer = await asyncio.open_unix_connection(str(tmp_path / "arb.sock"))
    await asyncio.sleep(0.05)
    b.publish(snap())
    line = await asyncio.wait_for(reader.readline(), timeout=2)
    assert json.loads(line)["kind"] == "snapshot"
    writer.close()
    await b.stop()


async def test_a_dropped_subscriber_is_announced_not_discarded_in_silence(tmp_path, capsys):
    """The eviction in `publish` used to be a bare `discard` + `close`.

    The subscriber it drops is the armed trader: it reads this socket, sees the
    close as `subscription ended (EOF)`, and pulls every resting quote plus a
    sweep of both venues before it can reconnect. That cost appeared nowhere on
    this side — the only trace in the entire system was on the client. This
    pins that it now says so, and names the numbers an operator needs.

    `MAX_BUFFER = -1` makes any buffer at all overflow, which is the whole
    condition under test; forcing a real 1 MB backlog would test the kernel's
    socket buffer instead.
    """
    b = UnixBroadcaster(tmp_path / "arb.sock")
    b.MAX_BUFFER = -1
    await b.start()
    reader, writer = await asyncio.open_unix_connection(str(tmp_path / "arb.sock"))
    await asyncio.sleep(0.05)
    assert len(b._writers) == 1
    b.publish(snap())
    assert b._writers == set(), "the over-buffered subscriber must be dropped"
    err = capsys.readouterr().err
    assert "DROPPED a subscriber" in err, f"the drop was silent: {err!r}"
    assert "-1" in err, "the line must name the limit that was crossed"
    writer.close()
    await b.stop()


async def test_polymarket_ws_task_records_from_fake_server(tmp_path):
    """Fake-WS harness (test mandate): serve book + price_change + trade,
    assert the recorder persists normalized events and stays subscribed."""
    got_subscribe = asyncio.Event()

    async def handler(ws):
        sub = json.loads(await ws.recv())
        assert sub["type"] == "market" and sub["assets_ids"] == ["111"]
        got_subscribe.set()
        await ws.send(
            json.dumps(
                {
                    "event_type": "book",
                    "asset_id": "111",
                    "bids": [{"price": "0.48", "size": "30"}],
                    "asks": [{"price": "0.52", "size": "25"}],
                    "timestamp": "1",
                }
            )
        )
        await ws.send(
            json.dumps(
                {
                    "event_type": "price_change",
                    "timestamp": "2",
                    "price_changes": [
                        {"asset_id": "111", "price": "0.49", "size": "10", "side": "BUY"}
                    ],
                }
            )
        )
        await ws.send(
            json.dumps(
                {
                    "event_type": "last_trade_price",
                    "asset_id": "111",
                    "price": "0.49",
                    "size": "5",
                    "side": "BUY",
                    "timestamp": "3",
                }
            )
        )
        await ws.wait_closed()  # hold open until the client disconnects

    async with websockets.serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        core = RecorderCore(JsonlWriter(tmp_path))
        liveness = LivenessTracker()
        task = asyncio.create_task(
            polymarket_ws_task(
                core, liveness, ["111"], ClobRest(), ws_url=f"ws://127.0.0.1:{port}"
            )
        )
        await asyncio.wait_for(got_subscribe.wait(), timeout=5)
        for _ in range(100):
            book = core.books.get("polymarket", "111")
            if book and book.seq >= 2:
                break
            await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    book = core.books.get("polymarket", "111")
    assert book is not None
    assert {l.price for l in book.bids} == {Decimal("0.48"), Decimal("0.49")}
    files = list(tmp_path.glob("polymarket-*.jsonl"))
    kinds = [raw["kind"] for _, raw in iter_jsonl(files[0])]
    assert kinds == ["snapshot", "delta", "trade"]
    assert liveness.check().get("polymarket-ws") is False  # fresh heartbeat
