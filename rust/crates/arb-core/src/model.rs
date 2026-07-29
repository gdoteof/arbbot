//! Wire models — byte-compatible with the Python recorder's pydantic output.
//!
//! Parity contract (pinned by tests against real tape lines): compact JSON,
//! field order = struct order with `kind` first, prices/sizes as
//! source-exponent decimal strings, `ts_venue` always present (null when
//! absent). scripts/bench_replay.py and the Python `parse_event` consume
//! these lines unmodified.

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Venue {
    #[serde(rename = "kalshi")]
    Kalshi,
    #[serde(rename = "polymarket")]
    Polymarket,
    #[serde(rename = "polymarket_us")]
    PolymarketUs,
}

impl Venue {
    pub fn as_str(&self) -> &'static str {
        match self {
            Venue::Kalshi => "kalshi",
            Venue::Polymarket => "polymarket",
            Venue::PolymarketUs => "polymarket_us",
        }
    }

    /// Inverse of [`Venue::as_str`], and the ONLY one — six hand-written copies
    /// of this match lived in six files (the trader engine, the intent replayer,
    /// the golden harness, the tob reader and both halves of the dash), so
    /// adding a venue meant finding all six and a venue string that reached the
    /// wrong copy parsed as `None`, which every one of those call sites reads as
    /// "skip this line".
    pub fn parse(s: &str) -> Option<Venue> {
        Some(match s {
            "kalshi" => Venue::Kalshi,
            "polymarket" => Venue::Polymarket,
            "polymarket_us" => Venue::PolymarketUs,
            _ => return None,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BookSide {
    #[serde(rename = "bid")]
    Bid,
    #[serde(rename = "ask")]
    Ask,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TakerSide {
    #[serde(rename = "buy")]
    Buy,
    #[serde(rename = "sell")]
    Sell,
}

/// Price/size carried as the exact decimal string that will hit the wire.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Level {
    pub price: String,
    pub size: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum TapeEvent {
    #[serde(rename = "snapshot")]
    Snapshot {
        venue: Venue,
        market_id: String,
        bids: Vec<Level>,
        asks: Vec<Level>,
        seq: u64,
        ts_local_ns: i64,
        ts_venue: Option<String>,
    },
    #[serde(rename = "delta")]
    Delta {
        venue: Venue,
        market_id: String,
        side: BookSide,
        price: String,
        size: String,
        seq: u64,
        ts_local_ns: i64,
        ts_venue: Option<String>,
    },
    #[serde(rename = "trade")]
    Trade {
        venue: Venue,
        market_id: String,
        price: String,
        size: String,
        taker_side: Option<TakerSide>,
        seq: u64,
        ts_local_ns: i64,
        ts_venue: Option<String>,
    },
}

impl TapeEvent {
    pub fn venue(&self) -> Venue {
        match self {
            TapeEvent::Snapshot { venue, .. }
            | TapeEvent::Delta { venue, .. }
            | TapeEvent::Trade { venue, .. } => *venue,
        }
    }

    pub fn market_id(&self) -> &str {
        match self {
            TapeEvent::Snapshot { market_id, .. }
            | TapeEvent::Delta { market_id, .. }
            | TapeEvent::Trade { market_id, .. } => market_id,
        }
    }

    pub fn to_json_line(&self) -> String {
        serde_json::to_string(self).expect("TapeEvent serializes")
    }
}

pub use crate::clock::now_local_ns;

#[cfg(test)]
mod tests {
    use super::*;

    // real lines from data/raw-sports/kalshi-2026-07-23.jsonl and the PM-US
    // tape — the byte-compatibility contract
    const DELTA: &str = r#"{"kind":"delta","venue":"kalshi","market_id":"KXITFMATCH-26JUL22DELHAZ-HAZ","side":"bid","price":"0.4800","size":"284.00","seq":2,"ts_local_ns":1784777866421156639,"ts_venue":null}"#;
    const TRADE: &str = r#"{"kind":"trade","venue":"kalshi","market_id":"KXMLSGAME-26JUL22LAFCRSL-TIE","price":"0.0700","size":"107.30","taker_side":"buy","seq":1,"ts_local_ns":1784777866422808165,"ts_venue":"1784777866192"}"#;
    const SNAP: &str = r#"{"kind":"snapshot","venue":"polymarket_us","market_id":"aec-atp-x","bids":[{"price":"0.6300","size":"33072.3800"}],"asks":[],"seq":7,"ts_local_ns":1,"ts_venue":"2026-07-23T01:02:03.123456789Z"}"#;

    #[test]
    fn round_trip_is_byte_identical() {
        for line in [DELTA, TRADE, SNAP] {
            let ev: TapeEvent = serde_json::from_str(line).unwrap();
            assert_eq!(ev.to_json_line(), line);
        }
    }

    /// `as_str` and `parse` are two matches over the same table, and the whole
    /// point of having one copy is that they cannot drift apart.
    #[test]
    fn every_venue_survives_a_string_round_trip() {
        for v in [Venue::Kalshi, Venue::Polymarket, Venue::PolymarketUs] {
            assert_eq!(Venue::parse(v.as_str()), Some(v));
        }
        assert_eq!(Venue::parse("polymarket_uk"), None);
        assert_eq!(Venue::parse(""), None);
    }
}
