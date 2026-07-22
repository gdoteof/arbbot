"""capital_snapshot.signal_flags — the two-tier unwind flag policy."""

import importlib.util
from decimal import Decimal
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "capital_snapshot", Path(__file__).parent.parent / "scripts" / "capital_snapshot.py")
cs = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cs)

D = Decimal


def test_hard_unwind_never_gated_on_utilization():
    assert cs.signal_flags(nu=2, nh=1, nr=0, util=D("15"), total=D("900")) \
        == ["UNWIND-HARD:1"]


def test_soft_unwind_gated_below_70pct():
    assert cs.signal_flags(nu=2, nh=0, nr=0, util=D("40"), total=D("900")) == []
    assert cs.signal_flags(nu=2, nh=0, nr=0, util=D("75"), total=D("900")) \
        == ["UNWIND-SIGNALS:2@util75%"]


def test_hard_positions_not_double_counted_in_soft():
    flags = cs.signal_flags(nu=3, nh=1, nr=0, util=D("80"), total=D("900"))
    assert flags == ["UNWIND-HARD:1", "UNWIND-SIGNALS:2@util80%"]


def test_reverse_always_flags():
    assert "REVERSE-SIGNALS:1" in cs.signal_flags(0, 0, 1, D("5"), D("900"))


def test_low_utilization_needs_material_capital():
    assert any(f.startswith("LOW-UTILIZATION") for f in
               cs.signal_flags(0, 0, 0, D("5"), D("900")))
    assert cs.signal_flags(0, 0, 0, D("5"), D("40")) == []  # tiny account: quiet
