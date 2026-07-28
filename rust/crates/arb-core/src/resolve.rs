//! Approximate resolution dates per relationship family, for hold-APR math.
//!
//! Port of `src/arbbot/exec/resolve_dates.py`. Lives in arb-core because more
//! than one binary needs it and they must not disagree: the trader uses it to
//! decide whether a crossing clears the APR bar, and the dashboard uses it to
//! report the APR of the trade that resulted. Two copies of this table would
//! mean the dash could disagree with the engine about what a trade was worth.
//!
//! Some events have no officially-fixed date far in advance (TIME Person of
//! the Year is announced ~2nd week of December; the French 2027 presidential
//! date is not set yet). Kalshi's `expected_expiration_time` is a conservative
//! year-end close, which UNDERSTATES time-to-resolution and so overstates APR.
//! These are curated best estimates, flagged `estimated` so a derived APR can
//! be shown as speculative rather than exact.

/// (relationship-id prefix, approximate resolve date, estimated?).
///
/// Python takes the FIRST prefix hit, not the longest, so order is
/// significant — keep it identical to `_RULES`.
const RULES: &[(&str, &str, bool)] = &[
    ("xvus-time-poty-26", "2026-12-09", true),
    ("xvus-france-pres-27", "2027-04-25", true),
    ("xvus-brazil-pres-26", "2026-10-25", true),
    ("xvus-nobel-peace-26", "2026-10-09", true),
    ("xvus-fedcut-26", "2026-12-31", false),
    ("xvus-btcmax-26-31", "2026-12-31", false),
];

/// `-> (YYYY-MM-DD, estimated)`. `None` when the family is unknown, which
/// callers must treat as "no APR", never as zero.
pub fn resolve_date(rel_id: &str) -> Option<(&'static str, bool)> {
    RULES
        .iter()
        .find(|(prefix, _, _)| rel_id.starts_with(prefix))
        .map(|(_, date, est)| (*date, *est))
}

/// Days since 1970-01-01 for a proleptic Gregorian date (Hinnant's
/// days_from_civil). Written out rather than pulling a date crate into a crate
/// whose whole point is having almost no dependencies.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Epoch day for a `YYYY-MM-DD` string.
pub fn parse_iso(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    if b.len() < 10 || b[4] != b'-' || b[7] != b'-' {
        return None;
    }
    let y: i64 = s.get(0..4)?.parse().ok()?;
    let m: i64 = s.get(5..7)?.parse().ok()?;
    let d: i64 = s.get(8..10)?.parse().ok()?;
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    Some(days_from_civil(y, m, d))
}

/// `YYYY-MM-DD` in UTC for an epoch-seconds instant.
pub fn today_iso(epoch_s: f64) -> String {
    let (y, m, d) = civil_from_days((epoch_s / 86400.0).floor() as i64);
    format!("{y:04}-{m:02}-{d:02}")
}

/// Years until the family's resolve date. Python floors the day count at 1
/// before dividing, so a same-day or already-past resolve yields a finite APR
/// rather than dividing by zero.
pub fn years_to(rel_id: &str, today_iso: &str) -> Option<f64> {
    let (rd, _) = resolve_date(rel_id)?;
    let days = parse_iso(rd)? - parse_iso(today_iso)?;
    Some(days.max(1) as f64 / 365.25)
}

/// Years between two `YYYY-MM-DD` dates, floored at one day. For callers that
/// already know the resolve date (a ledger record can carry its own).
pub fn years_between(from_iso: &str, to_iso: &str) -> Option<f64> {
    let days = parse_iso(to_iso)? - parse_iso(from_iso)?;
    Some(days.max(1) as f64 / 365.25)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rules_match_python() {
        assert_eq!(resolve_date("xvus-nobel-peace-26-elonmusk"), Some(("2026-10-09", true)));
        assert_eq!(resolve_date("xvus-fedcut-26-usfed-2026-cut"), Some(("2026-12-31", false)));
        assert_eq!(resolve_date("xvus-unknown-family-99"), None);
    }

    #[test]
    fn civil_days_are_exact() {
        assert_eq!(parse_iso("1970-01-01"), Some(0));
        assert_eq!(parse_iso("2026-07-28").unwrap() - parse_iso("2026-07-27").unwrap(), 1);
        assert_eq!(parse_iso("2028-03-01").unwrap() - parse_iso("2028-02-28").unwrap(), 2);
        assert_eq!(parse_iso("garbage"), None);
    }

    #[test]
    fn today_iso_round_trips() {
        let d = parse_iso("2026-07-28").unwrap() as f64 * 86400.0;
        assert_eq!(today_iso(d), "2026-07-28");
        assert_eq!(today_iso(d + 86399.0), "2026-07-28");
        assert_eq!(today_iso(d + 86400.0), "2026-07-29");
        assert_eq!(today_iso(0.0), "1970-01-01");
    }

    #[test]
    fn years_floor_at_one_day() {
        let y = years_to("xvus-nobel-peace-26-elonmusk", "2026-10-09").unwrap();
        assert!((y - 1.0 / 365.25).abs() < 1e-9, "same-day resolves floor to 1 day, got {y}");
        assert!(years_to("xvus-nobel-peace-26-elonmusk", "2027-01-01").unwrap() > 0.0);
        assert!(years_between("2026-07-28", "2027-07-28").unwrap() - 1.0 < 0.01);
    }
}
