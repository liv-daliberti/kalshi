-- ========= Core metadata =========

CREATE TABLE IF NOT EXISTS events (
  event_ticker TEXT PRIMARY KEY,
  series_ticker TEXT,
  title TEXT,
  sub_title TEXT,
  category TEXT,
  mutually_exclusive BOOLEAN,
  collateral_return_type TEXT,
  available_on_brokers BOOLEAN,
  product_metadata JSONB,
  strike_date TIMESTAMPTZ,
  strike_period TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS markets (
  ticker TEXT PRIMARY KEY,
  event_ticker TEXT REFERENCES events(event_ticker),
  market_type TEXT,
  title TEXT,
  subtitle TEXT,
  yes_sub_title TEXT,
  no_sub_title TEXT,
  category TEXT,
  response_price_units TEXT,

  created_time TIMESTAMPTZ,
  open_time TIMESTAMPTZ,
  close_time TIMESTAMPTZ,
  expiration_time TIMESTAMPTZ,
  latest_expiration_time TIMESTAMPTZ,
  expected_expiration_time TIMESTAMPTZ,

  settlement_timer_seconds INTEGER,
  can_close_early BOOLEAN,
  early_close_condition TEXT,

  rules_primary TEXT,
  rules_secondary TEXT,

  tick_size INTEGER,
  risk_limit_cents INTEGER,

  price_level_structure TEXT,
  price_ranges JSONB,

  strike_type TEXT,
  floor_strike INTEGER,
  cap_strike INTEGER,
  functional_strike TEXT,
  custom_strike JSONB,

  mve_collection_ticker TEXT,
  mve_selected_legs JSONB,
  primary_participant_key TEXT,

  settlement_value BIGINT,
  settlement_value_dollars NUMERIC(18,4),
  settlement_ts TIMESTAMPTZ,

  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_markets_event_ticker ON markets(event_ticker);

-- ========= Active set (what we subscribe to in WS) =========

CREATE TABLE IF NOT EXISTS active_markets (
  ticker TEXT PRIMARY KEY REFERENCES markets(ticker),
  event_ticker TEXT NOT NULL REFERENCES events(event_ticker),
  close_time TIMESTAMPTZ,
  status TEXT,
  last_seen_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_active_markets_event ON active_markets(event_ticker);
CREATE INDEX IF NOT EXISTS idx_active_markets_close_time ON active_markets(close_time);

-- ========= State / cursors =========

CREATE TABLE IF NOT EXISTS ingest_state (
  key TEXT PRIMARY KEY,
  value TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ========= Real-time ticks (WS ticker messages) =========
-- WS ticker msg includes price/yes_bid/yes_ask/volume/open_interest + ts :contentReference[oaicite:11]{index=11}
-- No uniqueness constraint: we keep every tick.

CREATE TABLE IF NOT EXISTS market_ticks (
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL,
  ticker TEXT NOT NULL REFERENCES markets(ticker),

  price INTEGER,
  yes_bid INTEGER,
  yes_ask INTEGER,

  price_dollars NUMERIC(18,4),
  yes_bid_dollars NUMERIC(18,4),
  yes_ask_dollars NUMERIC(18,4),
  no_bid_dollars NUMERIC(18,4),

  volume BIGINT,
  open_interest BIGINT,

  dollar_volume BIGINT,
  dollar_open_interest BIGINT,

  implied_yes_mid NUMERIC(18,6),

  sid INTEGER,
  raw JSONB
);

CREATE INDEX IF NOT EXISTS idx_market_ticks_ticker_ts ON market_ticks(ticker, ts DESC);

-- ========= Lifecycle log (WS market_lifecycle_v2) =========
-- Includes event_type + open_ts/close_ts + additional_metadata :contentReference[oaicite:12]{index=12}

CREATE TABLE IF NOT EXISTS lifecycle_events (
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  market_ticker TEXT,
  event_type TEXT NOT NULL,
  open_ts BIGINT,
  close_ts BIGINT,
  raw JSONB
);

CREATE INDEX IF NOT EXISTS idx_lifecycle_events_market ON lifecycle_events(market_ticker, ts DESC);

-- ========= Candlestick history (REST backfill) =========
-- Candlestick endpoint supports period_interval ∈ {1,60,1440} minutes :contentReference[oaicite:13]{index=13}

CREATE TABLE IF NOT EXISTS market_candles (
  market_ticker TEXT NOT NULL REFERENCES markets(ticker),
  period_interval_minutes INTEGER NOT NULL,      -- 1 / 60 / 1440
  end_period_ts TIMESTAMPTZ NOT NULL,            -- inclusive end (from API)
  start_period_ts TIMESTAMPTZ NOT NULL,          -- derived

  -- extracted from "price" distribution (dollars) when present
  open NUMERIC(18,4),
  high NUMERIC(18,4),
  low  NUMERIC(18,4),
  close NUMERIC(18,4),

  volume BIGINT,
  open_interest BIGINT,

  raw JSONB,
  PRIMARY KEY (market_ticker, period_interval_minutes, end_period_ts)
);

CREATE INDEX IF NOT EXISTS idx_market_candles_market_end ON market_candles(market_ticker, end_period_ts DESC);

-- ========= Convenience views =========

-- Latest tick per market, then join to event => “outcomes latest”
CREATE OR REPLACE VIEW event_outcomes_latest AS
SELECT
  e.event_ticker,
  e.title        AS event_title,
  e.category     AS event_category,
  e.strike_date,
  e.strike_period,

  m.ticker       AS market_ticker,
  m.title        AS outcome_title,
  m.subtitle     AS outcome_subtitle,

  t.ts           AS last_ts,
  t.implied_yes_mid,
  t.price_dollars,
  t.yes_bid_dollars,
  t.yes_ask_dollars,
  t.volume,
  t.open_interest
FROM events e
JOIN markets m ON m.event_ticker = e.event_ticker
JOIN LATERAL (
  SELECT *
  FROM market_ticks t
  WHERE t.ticker = m.ticker
  ORDER BY t.ts DESC, t.id DESC
  LIMIT 1
) t ON TRUE;

-- “Leader” per event by highest implied YES mid
CREATE OR REPLACE VIEW event_leader_latest AS
SELECT DISTINCT ON (event_ticker)
  event_ticker,
  event_title,
  event_category,
  strike_date,
  strike_period,
  market_ticker AS leader_market_ticker,
  outcome_title AS leader_outcome_title,
  implied_yes_mid AS leader_implied_yes_mid,
  last_ts
FROM event_outcomes_latest
WHERE implied_yes_mid IS NOT NULL
ORDER BY event_ticker, implied_yes_mid DESC, last_ts DESC;
