"""sports_arb.py PM US quirks (card 7fab301e, pmus-ioc-fill-report-lag):
PM's fill reporting can lag an IOC by seconds — a premature 0 read caused the
2026-07-22 refire runaway (130 shorts from a 2-lot hedge). _confirm_ioc_fill
must keep polling filled_qty and, after the polls, confirm against the
authoritative order record (cumQuantity) before concluding "unfilled".
Fakes only; nothing touches a venue.
"""

import importlib.util
import pathlib

spec = importlib.util.spec_from_file_location(
    "sports_arb", pathlib.Path(__file__).parent.parent / "scripts" / "sports_arb.py")
sa = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sa)


class LaggyGateway:
    """filled_qty reports 0 for the first `lag` calls, then `qty` (the venue's
    async fill reporting); get_order carries the authoritative cumQuantity."""

    def __init__(self, qty, lag, cum=None, fail=False):
        self.qty = qty
        self.lag = lag
        self.cum = cum
        self.fail = fail
        self.calls = 0

    def filled_qty(self, oid):
        if self.fail:
            raise RuntimeError("venue unreachable")
        self.calls += 1
        return self.qty if self.calls > self.lag else 0

    def get_order(self, oid):
        if self.cum is None:
            raise RuntimeError("venue unreachable")
        return {"id": oid, "cumQuantity": str(self.cum)}


def test_fill_reported_after_lag_is_found_by_polling():
    pg = LaggyGateway(qty=4, lag=3)
    assert sa._confirm_ioc_fill(pg, "OID", 4, tries=10, wait=0) == 4
    assert pg.calls == 4, "must poll past the lag, not trust the first 0"


def test_partial_fill_confirmed_via_cumquantity_after_polls():
    # polls never reach the requested qty (partial fill) — the post-poll
    # get_order confirm must surface the true cumQuantity, not 0
    pg = LaggyGateway(qty=0, lag=0, cum=2)
    assert sa._confirm_ioc_fill(pg, "OID", 4, tries=3, wait=0) == 2


def test_all_reads_failing_returns_zero_not_crash():
    pg = LaggyGateway(qty=0, lag=0, cum=None, fail=True)
    assert sa._confirm_ioc_fill(pg, "OID", 4, tries=3, wait=0) == 0
