//! Raw-tape Parquet reader.
//!
//! Unlike the scan tape, the raw tape CANNOT skip its nested columns: `bids`
//! and `asks` are `LIST<STRUCT<price,size>>` and they are the snapshot. So this
//! reads full rows and reconstructs a `TapeEvent`, rather than projecting a
//! flat subset the way `arb-query::opps` does.
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

/// `LIST<STRUCT<price,size>>` -> levels. Anything that is not a group with both
/// fields is skipped rather than guessed at.
fn levels(row: &Row, name: &str) -> Vec<Level> {
    let mut out = Vec::new();
    for (n, f) in row.get_column_iter() {
        if n != name {
            continue;
        }
        if let Field::ListInternal(list) = f {
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

    #[test]
    fn venue_names_match_the_tape_vocabulary() {
        assert_eq!(Venue::parse("kalshi"), Some(Venue::Kalshi));
        assert_eq!(Venue::parse("polymarket"), Some(Venue::Polymarket));
        assert_eq!(Venue::parse("polymarket_us"), Some(Venue::PolymarketUs));
        assert_eq!(Venue::parse("nope"), None);
    }
}
