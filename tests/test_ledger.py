from arbbot.exec.ledger import open_baskets, parse_lines


def _open(rid, ts, qty, cost=10.0):
    return {"ts": ts, "relationship_id": rid, "status": "open", "qty": qty,
            "cost_usd": cost, "payoff_usd": qty * 1.0, "profit_usd": qty * 0.05}


def _unwind(rid, closes_ts, qty):
    return {"ts": closes_ts + 100, "relationship_id": rid, "strategy": "unwind",
            "status": "unwound", "closes_ts": closes_ts, "qty": qty}


def test_no_unwinds_passthrough():
    recs = [_open("a", 1.0, 10), _open("b", 2.0, 5)]
    assert open_baskets(recs) == recs


def test_full_unwind_drops_basket():
    recs = [_open("a", 1.0, 10), _unwind("a", 1.0, 10)]
    assert open_baskets(recs) == []


def test_partial_unwind_scales():
    recs = [_open("a", 1.0, 10, cost=10.0), _unwind("a", 1.0, 4)]
    (b,) = open_baskets(recs)
    assert b["qty"] == 6
    assert abs(b["cost_usd"] - 6.0) < 1e-9
    assert abs(b["profit_usd"] - 0.3) < 1e-9


def test_unwind_only_matches_same_ts_and_rel():
    recs = [_open("a", 1.0, 10), _open("a", 2.0, 7), _unwind("a", 1.0, 10),
            _unwind("b", 2.0, 7)]
    (b,) = open_baskets(recs)
    assert b["ts"] == 2.0 and b["qty"] == 7


def test_multiple_partial_unwinds_accumulate():
    recs = [_open("a", 1.0, 10), _unwind("a", 1.0, 3), _unwind("a", 1.0, 7)]
    assert open_baskets(recs) == []


def test_parse_lines_skips_garbage():
    recs = parse_lines(['{"ts": 1}', "", "not json"])
    assert recs == [{"ts": 1}]


def test_correction_folds_and_hides():
    from arbbot.exec.ledger import apply_corrections
    recs = [_open("a", 1.0, 5, cost=6.0),
            {"ts": 2.0, "relationship_id": "a", "status": "correction",
             "corrects_ts": 1.0, "fields": {"cost_usd": 4.66, "profit_usd": 0.34}}]
    out = apply_corrections(recs)
    assert len(out) == 1
    assert out[0]["cost_usd"] == 4.66 and out[0]["profit_usd"] == 0.34
    assert out[0]["qty"] == 5  # untouched fields survive


def test_open_baskets_applies_corrections():
    recs = [_open("a", 1.0, 5, cost=6.0),
            {"ts": 2.0, "relationship_id": "a", "status": "correction",
             "corrects_ts": 1.0, "fields": {"cost_usd": 4.66}}]
    (b,) = open_baskets(recs)
    assert b["cost_usd"] == 4.66
