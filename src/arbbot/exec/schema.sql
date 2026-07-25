-- Trading-state schema for data/exec/trading.db (SQLite, WAL).
-- Shared verbatim by Python (sqlite3) and Rust (rusqlite). Idempotent:
-- every statement is CREATE ... IF NOT EXISTS; version via PRAGMA user_version.
--
-- Conventions:
--   *_usd_u  INTEGER micro-USD          (1_000_000 = $1)
--   *_pu     INTEGER micro-probability  (1_000_000 = 1.00)
--   *_ns     INTEGER ns since epoch
--   legacy_ts REAL — original JSONL float ts, the historical (rel_id, ts)
--             match key; kept verbatim for imported rows.
-- Economic events are append-only: basket/leg rows are never numerically
-- edited except as corrections (plain UPDATE inside a txn, before-image
-- captured by audit triggers). Open qty decreases ONLY via basket_close.

CREATE TABLE IF NOT EXISTS basket (
  basket_id       INTEGER PRIMARY KEY,
  idempotency_key TEXT NOT NULL UNIQUE,
  ts_ns           INTEGER NOT NULL,
  legacy_ts       REAL,
  relationship_id TEXT,
  strategy        TEXT,
  title           TEXT,
  qty             INTEGER NOT NULL CHECK (qty > 0),
  cost_usd_u      INTEGER,
  payoff_usd_u    INTEGER,
  profit_usd_u    INTEGER,
  resolves_by     TEXT,
  resolves_estimated INTEGER NOT NULL DEFAULT 0,
  hedge_latency_ms REAL,
  note            TEXT,
  voided          INTEGER NOT NULL DEFAULT 0,  -- correction rewrote status away from 'open'
  source          TEXT NOT NULL,
  raw_json        TEXT
);
CREATE INDEX IF NOT EXISTS ix_basket_rel ON basket(relationship_id);
CREATE INDEX IF NOT EXISTS ix_basket_ts  ON basket(ts_ns);

CREATE TABLE IF NOT EXISTS leg (
  basket_id   INTEGER NOT NULL REFERENCES basket(basket_id),
  leg_index   INTEGER NOT NULL,
  venue       TEXT NOT NULL CHECK (venue IN ('kalshi','polymarket','polymarket_us')),
  market_id   TEXT NOT NULL,
  side        TEXT,
  role        TEXT,
  qty         INTEGER NOT NULL,
  price_pu    INTEGER,             -- unified yes-equivalent price (was yes_price OR avg_price)
  cost_usd_u  INTEGER,
  fees_usd_u  INTEGER NOT NULL DEFAULT 0,
  venue_order_id  TEXT,
  price_estimated INTEGER NOT NULL DEFAULT 0,
  raw_json    TEXT,
  PRIMARY KEY (basket_id, leg_index)
);
CREATE INDEX IF NOT EXISTS ix_leg_market ON leg(venue, market_id);

-- The ONLY way open qty decreases. basket_id NULL = standalone realized
-- P&L event (e.g. naked-settlement of an untracked hold) or an orphan
-- import whose open record could not be matched — visible, never dropped.
CREATE TABLE IF NOT EXISTS basket_close (
  close_id        INTEGER PRIMARY KEY,
  idempotency_key TEXT NOT NULL UNIQUE,
  basket_id       INTEGER REFERENCES basket(basket_id),
  ts_ns           INTEGER NOT NULL,
  legacy_ts       REAL,
  kind            TEXT NOT NULL CHECK (kind IN
                    ('unwind','settlement','naked-settlement','flatten','expiry')),
  qty             INTEGER NOT NULL CHECK (qty > 0),
  realized_pnl_usd_u INTEGER,
  proceeds_usd_u  INTEGER,
  fees_usd_u      INTEGER NOT NULL DEFAULT 0,
  note            TEXT,
  source          TEXT NOT NULL,
  raw_json        TEXT
);
CREATE INDEX IF NOT EXISTS ix_close_basket ON basket_close(basket_id);

-- Correction audit. The triggers capture a before-image on EVERY update —
-- including rogue manual ones, which land with actor NULL (a trace, not a
-- block). ledgerdb.apply_correction annotates its own rows with actor and
-- reason inside the same transaction. (Triggers cannot read temp tables,
-- so per-connection context is annotated after the fact, not injected.)
CREATE TABLE IF NOT EXISTS audit_log (
  audit_id    INTEGER PRIMARY KEY,
  ts_ns       INTEGER NOT NULL,
  tbl         TEXT NOT NULL,
  pk          TEXT NOT NULL,
  actor       TEXT,
  reason      TEXT,
  before_json TEXT NOT NULL
);

CREATE TRIGGER IF NOT EXISTS trg_basket_audit BEFORE UPDATE ON basket BEGIN
  INSERT INTO audit_log(ts_ns, tbl, pk, before_json)
  VALUES (CAST(unixepoch('subsec') * 1e9 AS INTEGER), 'basket', OLD.basket_id,
          json_object('qty', OLD.qty, 'cost_usd_u', OLD.cost_usd_u,
                      'payoff_usd_u', OLD.payoff_usd_u, 'profit_usd_u', OLD.profit_usd_u,
                      'strategy', OLD.strategy, 'resolves_by', OLD.resolves_by,
                      'voided', OLD.voided, 'note', OLD.note));
END;

CREATE TRIGGER IF NOT EXISTS trg_leg_audit BEFORE UPDATE ON leg BEGIN
  INSERT INTO audit_log(ts_ns, tbl, pk, before_json)
  VALUES (CAST(unixepoch('subsec') * 1e9 AS INTEGER), 'leg',
          OLD.basket_id || ':' || OLD.leg_index,
          json_object('qty', OLD.qty, 'price_pu', OLD.price_pu,
                      'cost_usd_u', OLD.cost_usd_u, 'fees_usd_u', OLD.fees_usd_u,
                      'venue_order_id', OLD.venue_order_id));
END;

-- Which process may place orders for a relationship. Replaces
-- grep-the-probe-logs coordination; every order-placing actor checks it.
CREATE TABLE IF NOT EXISTS ownership (
  relationship_id TEXT PRIMARY KEY,
  owner           TEXT NOT NULL,     -- 'python-trader' | 'rust-trader' | 'probe-<name>' | ...
  updated_ns      INTEGER NOT NULL,
  note            TEXT
);

-- Recon guard state (replaces the four .recon_*.json blobs).
CREATE TABLE IF NOT EXISTS recon_baseline (
  venue       TEXT PRIMARY KEY,
  n_positions INTEGER NOT NULL,
  updated_ns  INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS recon_sighting (
  imbalance_key TEXT PRIMARY KEY,
  kalshi_ticker TEXT,
  first_seen_ns INTEGER NOT NULL,
  last_seen_ns  INTEGER NOT NULL,
  runs          INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS recon_ignore (
  fragment    TEXT PRIMARY KEY,
  max_imb     INTEGER NOT NULL,
  expires_ns  INTEGER NOT NULL,
  added_by    TEXT,
  note        TEXT
);
CREATE TABLE IF NOT EXISTS recon_incident (
  incident_id  INTEGER PRIMARY KEY,
  ts_ns        INTEGER NOT NULL,
  venue        TEXT,
  n_positions  INTEGER,
  baseline     INTEGER,
  missing_json TEXT
);

-- Watched naked holds (replaces sports_naked.json).
CREATE TABLE IF NOT EXISTS naked_hold (
  kalshi_ticker TEXT NOT NULL,
  side          TEXT NOT NULL,
  qty           INTEGER NOT NULL,
  ref_px_pu     INTEGER,
  game          TEXT,
  added_ns      INTEGER NOT NULL,
  PRIMARY KEY (kalshi_ticker, side)
);

-- Venue-truth snapshots (replace positions.json / marks.json as state).
CREATE TABLE IF NOT EXISTS position_snapshot (
  snap_ns   INTEGER NOT NULL,
  venue     TEXT NOT NULL,
  market_id TEXT NOT NULL,
  qty       REAL NOT NULL,
  basis_ct_u INTEGER,
  bid_pu    INTEGER,
  ask_pu    INTEGER,
  status    TEXT,
  PRIMARY KEY (snap_ns, venue, market_id)
);
CREATE TABLE IF NOT EXISTS mark (
  venue     TEXT NOT NULL,
  market_id TEXT NOT NULL,
  ts_ns     INTEGER NOT NULL,
  mark_pu   INTEGER,
  source    TEXT,
  PRIMARY KEY (venue, market_id)
);

-- Per-day tape archive accounting (replaces archived_counts.json).
CREATE TABLE IF NOT EXISTS tape_archive (
  day  TEXT NOT NULL,
  stem TEXT NOT NULL,
  rows INTEGER NOT NULL,
  archived_ns INTEGER NOT NULL,
  PRIMARY KEY (day, stem)
);

-- Cross-process venue API rate budget (token bucket, one row per venue).
-- Consumed by arbbot.venues.pmus.consume_budget on every background request;
-- critical order-path calls bypass it entirely (they never open the DB).
CREATE TABLE IF NOT EXISTS rate_budget (
  venue           TEXT PRIMARY KEY,
  window_start_ns INTEGER NOT NULL,
  used            INTEGER NOT NULL
);

-- ============ views: the read contract ============
-- Python scripts and the dashboard query these, nothing else.

-- Open qty net of closes; economics scaled proportionally.
-- Mirrors exec/ledger.py open_baskets() exactly (frozen reference).
DROP VIEW IF EXISTS v_open_baskets;
CREATE VIEW v_open_baskets AS
SELECT b.*,
       b.qty - IFNULL(c.closed, 0) AS open_qty,
       CAST(ROUND(b.cost_usd_u   * (b.qty - IFNULL(c.closed,0)) / CAST(b.qty AS REAL)) AS INTEGER) AS open_cost_usd_u,
       CAST(ROUND(b.payoff_usd_u * (b.qty - IFNULL(c.closed,0)) / CAST(b.qty AS REAL)) AS INTEGER) AS open_payoff_usd_u,
       CAST(ROUND(b.profit_usd_u * (b.qty - IFNULL(c.closed,0)) / CAST(b.qty AS REAL)) AS INTEGER) AS open_profit_usd_u
FROM basket b
LEFT JOIN (SELECT basket_id, SUM(qty) AS closed
           FROM basket_close WHERE basket_id IS NOT NULL
           GROUP BY basket_id) c USING (basket_id)
WHERE b.voided = 0 AND b.qty - IFNULL(c.closed, 0) > 0;

DROP VIEW IF EXISTS v_open_legs;
CREATE VIEW v_open_legs AS
SELECT l.*, v.open_qty, v.relationship_id, v.strategy
FROM leg l JOIN v_open_baskets v USING (basket_id);

-- Net expected contracts per venue market (recon's "expected" set).
DROP VIEW IF EXISTS v_position_by_market;
CREATE VIEW v_position_by_market AS
SELECT venue, market_id,
       SUM(CASE WHEN side = 'yes' THEN open_qty ELSE -open_qty END) AS net_yes_qty,
       SUM(open_qty) AS gross_qty
FROM v_open_legs GROUP BY venue, market_id;

DROP VIEW IF EXISTS v_realized_pnl;
CREATE VIEW v_realized_pnl AS
SELECT date(ts_ns / 1000000000, 'unixepoch') AS day, kind,
       SUM(realized_pnl_usd_u) AS pnl_usd_u, COUNT(*) AS n
FROM basket_close GROUP BY 1, 2;

-- ============ registry: compiled relationship universe ============
-- Written ONLY by scripts/compile_registry.py, which validates the YAML
-- registries (config/registry.yaml + generated data/registry/*.yaml) through
-- the pydantic model and rewrites both tables wholesale in one transaction.
-- category/topic are stamped at compile time (arbbot.registry.categorize
-- when the YAML doesn't carry explicit fields).

CREATE TABLE IF NOT EXISTS registry (
  id          TEXT PRIMARY KEY,
  type        TEXT NOT NULL,
  category    TEXT NOT NULL,
  topic       TEXT NOT NULL,
  verdict     TEXT NOT NULL,
  vetted_by   TEXT NOT NULL,
  oracle_risk TEXT NOT NULL,
  tranche     TEXT NOT NULL,
  event_date  TEXT,
  direction   TEXT,
  confidence  REAL,
  vetted_at   TEXT,
  source_file TEXT NOT NULL,
  source_git_hash TEXT,
  compiled_ns INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS registry_leg (
  rel_id       TEXT NOT NULL REFERENCES registry(id),
  leg_index    INTEGER NOT NULL,
  venue        TEXT NOT NULL CHECK (venue IN ('kalshi','polymarket','polymarket_us')),
  market_id    TEXT NOT NULL,
  no_market_id TEXT,               -- venue's NO-side id where distinct (PM tokens)
  side         TEXT NOT NULL,
  role         TEXT NOT NULL,
  PRIMARY KEY (rel_id, leg_index)
);
CREATE INDEX IF NOT EXISTS ix_registry_leg_market ON registry_leg(venue, market_id);

-- The tradable gate: human-vetted equivalences (Relationship.tradable), plus
-- the mechanical sports carve-out (rule-matched cross-venue game markets).
DROP VIEW IF EXISTS v_tradable_relationships;
CREATE VIEW v_tradable_relationships AS
SELECT * FROM registry
WHERE verdict IN ('equivalent', 'equivalent-with-caveat')
  AND (vetted_by = 'human'
       OR (vetted_by = 'mechanical'
           AND type = 'cross-venue-equivalent'
           AND category = 'sports'));
