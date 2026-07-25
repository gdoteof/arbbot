//! arb-core: pure decision/model core — zero I/O dependencies by design.
//! Determinism and tape parity are enforced by the crate graph: this crate
//! depends only on serde/serde_json.

pub mod book;
pub mod fees;
pub mod fill;
pub mod scan;
pub mod dec;
pub mod model;
pub mod quoter;
pub mod risk;
