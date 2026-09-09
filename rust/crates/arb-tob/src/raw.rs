//! Raw-tape Parquet reader.
//!
//! Unlike the scan tape, the raw tape CANNOT skip its `bids` and `asks`
//! columns: they ARE the snapshot. So this reads full rows and reconstructs a
//! `TapeEvent`, rather than projecting a flat subset the way
//! `arb-query::opps` does.
//!
//! Those two columns are `OPTIONAL BYTE_ARRAY (UTF8)` holding the JSON array
//! verbatim — the archiver is DuckDB and that is what it writes. This module
//! previously documented and read them as `LIST<STRUCT<price,size>>`, which no
//! file in `data/parquet` has ever used; see [`levels`].
//!
//! Row kinds are heterogeneous and each venue emits a different mix — PM-US is
//! snapshot-only, Kalshi delta+trade — so every field is optional and the
//! `kind` discriminator decides which are required. That heterogeneity is the
//! same thing that broke the Python archiver's global required-column check.

use std::fs::File;
use std::path::Path;

use arb_core::model::{BookSide, Level, TakerSide, TapeEvent, Venue};
use parquet::file::reader::SerializedFileReader;
use parquet::record::{Field, Row};

fn str_field(row: &Row, name: &str) -> Option<String> {
    for (n, f) in row.get_column_iter() {
        if n == name {
            return match f {
                Field::Str(s) => Some(s.clone()),
                Field::Bytes(b) => String::from_utf8(b.data().to_vec()).ok(),
                _ => None,
            };
        }
    }
    None
}

fn i64_field(row: &Row, name: &str) -> Option<i64> {
    for (n, f) in row.get_column_iter() {
        if n == name {
            return match f {
                Field::Long(v) => Some(*v),
                Field::Int(v) => Some(*v as i64),
                _ => None,
            };
        }
    }
    None
}

/// One `[{"price":..,"size":..}]` array as the archiver writes it.
///
/// Both members are quoted strings on the tape and are kept as strings all the
/// way to `dec`, so they are read as strings here. A level missing either one
/// is skipped rather than guessed at.
fn parse_levels_json(s: &str) -> Vec<Level> {
    let Ok(serde_json::Value::Array(items)) = serde_json::from_str::<serde_json::Value>(s) else {
        return Vec::new();
    };
    items
        .iter()
        .filter_map(|v| {
            let price = v.get("price")?.as_str()?.to_string();
            let size = v.get("size")?.as_str()?.to_string();
            Some(Level { price, size })
        })
        .collect()
}

/// The `bids`/`asks` column -> levels.
///
/// TWO encodings are accepted because the archiver's is not the one this
/// module was written against. DuckDB writes the column as
/// `OPTIONAL BYTE_ARRAY (UTF8)` holding the JSON array verbatim, NOT as the
/// `LIST<STRUCT<price,size>>` the module doc claimed — every file in
/// `data/parquet` is the string shape, so the list arm below had never matched
/// a production row. It failed silently: a `Field::Str` fell through to an
/// empty `Vec`, which is a legal empty book, so PM-US replayed as 88 samples a
/// day (one per market, all `bid: null`) with `gaps 0` and `bad 0` while
/// Kalshi — whose book is built from flat `delta` rows that never touch this
/// function — looked perfectly healthy.
///
/// The list arm is kept: it costs one match arm and is what a non-DuckDB
/// archiver would emit.
fn levels(row: &Row, name: &str) -> Vec<Level> {
    let mut out = Vec::new();
    for (n, f) in row.get_column_iter() {
        if n != name {
            continue;
        }
        match f {
            // The live shape.
            Field::Str(s) => out.extend(parse_levels_json(s)),
            Field::Bytes(b) => {
                if let Ok(s) = std::str::from_utf8(b.data()) {
                    out.extend(parse_levels_json(s));
                }
            }
            Field::ListInternal(list) => {
                for el in list.elements() {
                    if let Field::Group(g) = el {
                        let price = str_field(g, "price");
                        let size = str_field(g, "size");
                        if let (Some(price), Some(size)) = (price, size) {
                            out.push(Level { price, size });
                        }
                    }
                }
            }
            _ => {}
        }
    }
    out
}

fn to_event(row: &Row) -> Option<TapeEvent> {
    let kind = str_field(row, "kind")?;
    let venue = Venue::parse(&str_field(row, "venue")?)?;
    let market_id = str_field(row, "market_id")?;
    let seq = i64_field(row, "seq").unwrap_or(0).max(0) as u64;
    let ts_local_ns = i64_field(row, "ts_local_ns").unwrap_or(0);
    let ts_venue = str_field(row, "ts_venue");

    match kind.as_str() {
        "snapshot" => Some(TapeEvent::Snapshot {
            venue,
            market_id,
            bids: levels(row, "bids"),
            asks: levels(row, "asks"),
            seq,
            ts_local_ns,
            ts_venue,
        }),
        "delta" => {
            let side = BookSide::parse(&str_field(row, "side")?)?;
            Some(TapeEvent::Delta {
                venue,
                market_id,
                side,
                price: str_field(row, "price")?,
                size: str_field(row, "size")?,
                seq,
                ts_local_ns,
                ts_venue,
            })
        }
        "trade" => Some(TapeEvent::Trade {
            venue,
            market_id,
            price: str_field(row, "price")?,
            size: str_field(row, "size")?,
            taker_side: str_field(row, "taker_side").and_then(|s| match s.as_str() {
                "buy" => Some(TakerSide::Buy),
                "sell" => Some(TakerSide::Sell),
                _ => None,
            }),
            seq,
            ts_local_ns,
            ts_venue,
        }),
        _ => None,
    }
}

/// Stream a raw-tape Parquet file as `TapeEvent`s. `Err(())` marks a row that
/// could not be reconstructed, so the caller can count it instead of losing it
/// silently.
pub fn iter_parquet(
    path: &Path,
) -> Result<Box<dyn Iterator<Item = Result<TapeEvent, ()>>>, String> {
    let f = File::open(path).map_err(|e| format!("open {}: {e}", path.display()))?;
    let reader = SerializedFileReader::new(f).map_err(|e| format!("{}: {e}", path.display()))?;
    let iter = reader
        .into_iter()
        .map(|r| r.ok().and_then(|row| to_event(&row)).ok_or(()));
    Ok(Box::new(iter))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The exact bytes DuckDB puts in the `bids` column, copied off
    /// `data/parquet/polymarket_us-2026-09-07.parquet`.
    ///
    /// This is the shape the archive is actually in. Reading it as anything
    /// else returns an EMPTY book, and an empty book is legal — that is why
    /// the wrong reader lost 21 PM-US days without a gap, a parse failure or a
    /// single log line to show for it.
    const ARCHIVED_BIDS: &str = r#"[{"price":"0.1200","size":"3027.0000"},{"price":"0.0900","size":"1.0000"},{"price":"0.0100","size":"707.0000"}]"#;

    #[test]
    fn reads_the_json_string_the_archiver_actually_writes() {
        let got = parse_levels_json(ARCHIVED_BIDS);
        assert_eq!(got.len(), 3, "all three levels, in book order");
        assert_eq!(got[0].price, "0.1200");
        assert_eq!(got[0].size, "3027.0000");
        assert_eq!(got[2].price, "0.0100");
    }

    #[test]
    fn prices_keep_their_tape_spelling() {
        // Trailing zeros survive: these strings travel to `dec` unmodified, and
        // re-formatting them here would be a second encoding of the same price.
        let got = parse_levels_json(r#"[{"price":"0.0500","size":"10.0000"}]"#);
        assert_eq!(got[0].price, "0.0500");
        assert_eq!(got[0].size, "10.0000");
    }

    #[test]
    fn a_level_missing_a_field_is_skipped_not_guessed() {
        let got = parse_levels_json(r#"[{"price":"0.10"},{"price":"0.09","size":"5"}]"#);
        assert_eq!(got.len(), 1, "only the complete level");
        assert_eq!(got[0].price, "0.09");
    }

    #[test]
    fn an_empty_book_and_unreadable_json_both_yield_no_levels() {
        assert!(parse_levels_json("[]").is_empty());
        assert!(parse_levels_json("").is_empty());
        assert!(parse_levels_json("not json").is_empty());
    }

    #[test]
    fn venue_names_match_the_tape_vocabulary() {
        assert_eq!(Venue::parse("kalshi"), Some(Venue::Kalshi));
        assert_eq!(Venue::parse("polymarket"), Some(Venue::Polymarket));
        assert_eq!(Venue::parse("polymarket_us"), Some(Venue::PolymarketUs));
        assert_eq!(Venue::parse("nope"), None);
    }
}
