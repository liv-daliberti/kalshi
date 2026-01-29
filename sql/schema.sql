-- ========= Core metadata =========

CREATE EXTENSION IF NOT EXISTS pg_trgm;

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
  search_text TEXT GENERATED ALWAYS AS (
    COALESCE(title, '') || ' ' ||
    COALESCE(sub_title, '') || ' ' ||
    COALESCE(event_ticker, '') || ' ' ||
    COALESCE(category, '')
  ) STORED,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE events
  ADD COLUMN IF NOT EXISTS search_text TEXT GENERATED ALWAYS AS (
    COALESCE(title, '') || ' ' ||
    COALESCE(sub_title, '') || ' ' ||
    COALESCE(event_ticker, '') || ' ' ||
    COALESCE(category, '')
  ) STORED;

CREATE INDEX IF NOT EXISTS idx_events_strike_period ON events(strike_period);
CREATE INDEX IF NOT EXISTS idx_events_category_lower ON events(LOWER(category));
CREATE INDEX IF NOT EXISTS idx_events_search_trgm
  ON events USING gin (search_text gin_trgm_ops);

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

  search_text TEXT GENERATED ALWAYS AS (
    COALESCE(title, '') || ' ' ||
    COALESCE(subtitle, '') || ' ' ||
    COALESCE(yes_sub_title, '') || ' ' ||
    COALESCE(ticker, '')
  ) STORED,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE markets
  ADD COLUMN IF NOT EXISTS search_text TEXT GENERATED ALWAYS AS (
    COALESCE(title, '') || ' ' ||
    COALESCE(subtitle, '') || ' ' ||
    COALESCE(yes_sub_title, '') || ' ' ||
    COALESCE(ticker, '')
  ) STORED;

CREATE INDEX IF NOT EXISTS idx_markets_event_ticker ON markets(event_ticker);
CREATE INDEX IF NOT EXISTS idx_markets_open_time ON markets(open_time);
CREATE INDEX IF NOT EXISTS idx_markets_close_time ON markets(close_time);
CREATE INDEX IF NOT EXISTS idx_markets_search_trgm
  ON markets USING gin (search_text gin_trgm_ops);

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
CREATE INDEX IF NOT EXISTS idx_active_markets_status ON active_markets(status);

-- ========= State / cursors =========

CREATE TABLE IF NOT EXISTS ingest_state (
  key TEXT PRIMARY KEY,
  value TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ========= Schema versioning =========

CREATE TABLE IF NOT EXISTS schema_version (
  version INTEGER PRIMARY KEY,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO schema_version(version)
VALUES (3)
ON CONFLICT DO NOTHING;

-- ========= Work queue (DB-backed) =========

CREATE TABLE IF NOT EXISTS work_queue (
  id BIGSERIAL PRIMARY KEY,
  job_type TEXT NOT NULL,
  payload JSONB NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  attempts INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 5,
  available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  locked_by TEXT,
  locked_at TIMESTAMPTZ,
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_work_queue_status_available
  ON work_queue(status, available_at);
CREATE INDEX IF NOT EXISTS idx_work_queue_locked_at
  ON work_queue(status, locked_at);

-- ========= Real-time ticks (WS ticker messages) =========
-- WS ticker msg includes price/yes_bid/yes_ask/volume/open_interest + ts :contentReference[oaicite:11]{index=11}
-- Partitioned by ts; primary key includes ts for partitioning (we still keep every tick).

CREATE TABLE IF NOT EXISTS market_ticks (
  id BIGSERIAL,
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
  raw JSONB,
  PRIMARY KEY (id, ts)
) PARTITION BY RANGE (ts);

CREATE INDEX IF NOT EXISTS idx_market_ticks_ticker_ts ON market_ticks(ticker, ts DESC);
CREATE INDEX IF NOT EXISTS idx_market_ticks_ts ON market_ticks(ts DESC);

DO $$
DECLARE
  base_date date := date_trunc('month', now())::date;
  part_start date;
  part_end date;
  part_name text;
  month_offset int;
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_partitioned_table
    WHERE partrelid = 'market_ticks'::regclass
  ) THEN
    EXECUTE 'CREATE TABLE IF NOT EXISTS market_ticks_default PARTITION OF market_ticks DEFAULT';
    FOR month_offset IN -2..2 LOOP
      part_start := (base_date + (month_offset || ' month')::interval)::date;
      part_end := (part_start + interval '1 month')::date;
      part_name := format('market_ticks_%s', to_char(part_start, 'YYYY_MM'));
      EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF market_ticks FOR VALUES FROM (%L) TO (%L)',
        part_name,
        part_start,
        part_end
      );
    END LOOP;
  END IF;
END $$;

-- ========= Latest tick per market (materialized) =========

CREATE TABLE IF NOT EXISTS market_ticks_latest (
  ticker TEXT PRIMARY KEY REFERENCES markets(ticker),
  ts TIMESTAMPTZ NOT NULL,
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
  raw JSONB,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_market_ticks_latest_ts ON market_ticks_latest(ts DESC);

-- ========= Event sparklines (precomputed) =========

CREATE TABLE IF NOT EXISTS event_sparkline (
  ticker TEXT PRIMARY KEY REFERENCES markets(ticker),
  points DOUBLE PRECISION[] NOT NULL DEFAULT '{}',
  last_ts TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_event_sparkline_updated_at
  ON event_sparkline(updated_at DESC);

CREATE OR REPLACE FUNCTION refresh_event_sparklines(
  p_max_points INTEGER,
  p_min_tick_ts TIMESTAMPTZ
) RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
  v_max_points INTEGER := GREATEST(COALESCE(p_max_points, 24), 4);
  v_count INTEGER := 0;
BEGIN
  WITH recent_tickers AS (
    SELECT ticker
    FROM market_ticks_latest
    WHERE p_min_tick_ts IS NULL OR ts >= p_min_tick_ts
  ),
  ranked AS (
    SELECT
      mt.ticker,
      mt.ts,
      CASE
        WHEN mt.implied_yes_mid IS NOT NULL THEN mt.implied_yes_mid
        WHEN mt.price_dollars IS NOT NULL THEN mt.price_dollars
        WHEN mt.yes_bid_dollars IS NOT NULL AND mt.yes_ask_dollars IS NOT NULL
          THEN (mt.yes_bid_dollars + mt.yes_ask_dollars) / 2
        WHEN mt.yes_bid_dollars IS NOT NULL THEN mt.yes_bid_dollars
        WHEN mt.yes_ask_dollars IS NOT NULL THEN mt.yes_ask_dollars
        ELSE NULL
      END AS value,
      ROW_NUMBER() OVER (
        PARTITION BY mt.ticker
        ORDER BY mt.ts DESC, mt.id DESC
      ) AS rn
    FROM market_ticks mt
    JOIN recent_tickers rt ON rt.ticker = mt.ticker
  ),
  limited AS (
    SELECT ticker, ts, value
    FROM ranked
    WHERE rn <= v_max_points
  ),
  aggregated AS (
    SELECT
      ticker,
      MAX(ts) AS last_ts,
      ARRAY_AGG(
        LEAST(GREATEST(value::double precision, 0.0), 1.0)
        ORDER BY ts
      ) FILTER (WHERE value IS NOT NULL) AS points
    FROM limited
    GROUP BY ticker
  )
  INSERT INTO event_sparkline (
    ticker,
    last_ts,
    points,
    updated_at
  )
  SELECT
    ticker,
    last_ts,
    COALESCE(points, '{}'::double precision[]),
    NOW()
  FROM aggregated
  ON CONFLICT (ticker) DO UPDATE SET
    last_ts = EXCLUDED.last_ts,
    points = EXCLUDED.points,
    updated_at = NOW();

  GET DIAGNOSTICS v_count = ROW_COUNT;
  RETURN v_count;
END;
$$;

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
) PARTITION BY RANGE (end_period_ts);

CREATE INDEX IF NOT EXISTS idx_market_candles_market_end ON market_candles(market_ticker, end_period_ts DESC);

DO $$
DECLARE
  base_date date := date_trunc('month', now())::date;
  part_start date;
  part_end date;
  part_name text;
  month_offset int;
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_partitioned_table
    WHERE partrelid = 'market_candles'::regclass
  ) THEN
    EXECUTE 'CREATE TABLE IF NOT EXISTS market_candles_default PARTITION OF market_candles DEFAULT';
    FOR month_offset IN -2..2 LOOP
      part_start := (base_date + (month_offset || ' month')::interval)::date;
      part_end := (part_start + interval '1 month')::date;
      part_name := format('market_candles_%s', to_char(part_start, 'YYYY_MM'));
      EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF market_candles FOR VALUES FROM (%L) TO (%L)',
        part_name,
        part_start,
        part_end
      );
    END LOOP;
  END IF;
END $$;

-- ========= Latest candle per market (materialized) =========

CREATE TABLE IF NOT EXISTS market_candles_latest (
  market_ticker TEXT PRIMARY KEY REFERENCES markets(ticker),
  period_interval_minutes INTEGER NOT NULL,
  end_period_ts TIMESTAMPTZ NOT NULL,
  start_period_ts TIMESTAMPTZ,
  open NUMERIC(18,4),
  high NUMERIC(18,4),
  low  NUMERIC(18,4),
  close NUMERIC(18,4),
  volume BIGINT,
  open_interest BIGINT,
  raw JSONB,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_market_candles_latest_end ON market_candles_latest(end_period_ts DESC);

-- ========= Agent predictions =========

CREATE TABLE IF NOT EXISTS prediction_runs (
  id BIGSERIAL PRIMARY KEY,
  event_ticker TEXT REFERENCES events(event_ticker),
  run_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  prompt TEXT,
  agent TEXT,
  model TEXT,
  status TEXT NOT NULL DEFAULT 'completed',
  error TEXT,
  metadata JSONB
);

CREATE INDEX IF NOT EXISTS idx_prediction_runs_event_ts ON prediction_runs(event_ticker, run_ts DESC);

CREATE TABLE IF NOT EXISTS market_predictions (
  id BIGSERIAL PRIMARY KEY,
  run_id BIGINT REFERENCES prediction_runs(id) ON DELETE SET NULL,
  event_ticker TEXT REFERENCES events(event_ticker),
  market_ticker TEXT REFERENCES markets(ticker),
  predicted_yes_prob NUMERIC(10,6),
  confidence NUMERIC(10,6),
  rationale TEXT,
  raw JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_market_predictions_market_ts ON market_predictions(market_ticker, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_market_predictions_event_ts ON market_predictions(event_ticker, created_at DESC);

-- ========= RAG documents =========

CREATE TABLE IF NOT EXISTS rag_documents (
  id BIGSERIAL PRIMARY KEY,
  source TEXT,
  source_id TEXT,
  event_ticker TEXT REFERENCES events(event_ticker),
  market_ticker TEXT REFERENCES markets(ticker),
  content TEXT NOT NULL,
  embedding JSONB,
  metadata JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_rag_documents_event ON rag_documents(event_ticker);
CREATE INDEX IF NOT EXISTS idx_rag_documents_market ON rag_documents(market_ticker);

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
JOIN market_ticks_latest t ON t.ticker = m.ticker;

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

-- ========= Portal snapshot helpers =========

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_class
    WHERE relname = 'portal_event_rollup'
      AND relkind = 'm'
  ) THEN
    EXECUTE 'DROP MATERIALIZED VIEW IF EXISTS portal_event_rollup';
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS portal_event_rollup (
  event_ticker TEXT NOT NULL,
  event_title TEXT,
  event_sub_title TEXT,
  event_category TEXT,
  strike_period TEXT,
  search_text TEXT GENERATED ALWAYS AS (
    COALESCE(event_title, '') || ' ' ||
    COALESCE(event_sub_title, '') || ' ' ||
    COALESCE(event_ticker, '') || ' ' ||
    COALESCE(event_category, '')
  ) STORED,
  open_time TIMESTAMPTZ,
  close_time TIMESTAMPTZ,
  market_count INTEGER NOT NULL DEFAULT 0,
  closed_market_count INTEGER NOT NULL DEFAULT 0,
  volume BIGINT,
  has_tick BOOLEAN NOT NULL DEFAULT FALSE,
  has_open_status BOOLEAN NOT NULL DEFAULT FALSE,
  has_paused_status BOOLEAN NOT NULL DEFAULT FALSE,
  status TEXT,
  is_active BOOLEAN NOT NULL DEFAULT FALSE,
  is_scheduled BOOLEAN NOT NULL DEFAULT FALSE,
  is_closed BOOLEAN NOT NULL DEFAULT FALSE,
  agent_yes_prob NUMERIC(10,6),
  agent_confidence NUMERIC(10,6),
  agent_prediction_ts TIMESTAMPTZ,
  agent_market_ticker TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT idx_portal_event_rollup_ticker PRIMARY KEY (event_ticker)
);

ALTER TABLE portal_event_rollup
  ADD COLUMN IF NOT EXISTS search_text TEXT GENERATED ALWAYS AS (
    COALESCE(event_title, '') || ' ' ||
    COALESCE(event_sub_title, '') || ' ' ||
    COALESCE(event_ticker, '') || ' ' ||
    COALESCE(event_category, '')
  ) STORED;
ALTER TABLE portal_event_rollup
  ADD COLUMN IF NOT EXISTS status TEXT,
  ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS is_scheduled BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS is_closed BOOLEAN NOT NULL DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS idx_portal_event_rollup_search_trgm
  ON portal_event_rollup USING gin (
    search_text gin_trgm_ops
  );

CREATE INDEX IF NOT EXISTS idx_portal_event_rollup_close_time_ticker
  ON portal_event_rollup (close_time, event_ticker);

CREATE INDEX IF NOT EXISTS idx_portal_event_rollup_open_time_ticker
  ON portal_event_rollup (open_time, event_ticker);
CREATE INDEX IF NOT EXISTS idx_portal_event_rollup_status
  ON portal_event_rollup (status);
CREATE INDEX IF NOT EXISTS idx_portal_event_rollup_is_active
  ON portal_event_rollup (is_active);
CREATE INDEX IF NOT EXISTS idx_portal_event_rollup_is_scheduled
  ON portal_event_rollup (is_scheduled);
CREATE INDEX IF NOT EXISTS idx_portal_event_rollup_is_closed
  ON portal_event_rollup (is_closed);
CREATE INDEX IF NOT EXISTS idx_portal_event_rollup_close_time
  ON portal_event_rollup (close_time);
CREATE INDEX IF NOT EXISTS idx_portal_event_rollup_open_time
  ON portal_event_rollup (open_time);
CREATE INDEX IF NOT EXISTS idx_portal_event_rollup_category
  ON portal_event_rollup (LOWER(event_category));
CREATE INDEX IF NOT EXISTS idx_portal_event_rollup_strike_period
  ON portal_event_rollup (LOWER(strike_period));

CREATE OR REPLACE FUNCTION portal_refresh_event_rollup(p_event_ticker TEXT)
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
DECLARE
  v_row RECORD;
BEGIN
  IF p_event_ticker IS NULL OR BTRIM(p_event_ticker) = '' THEN
    RETURN;
  END IF;
  IF NOT pg_try_advisory_xact_lock(hashtext(p_event_ticker)) THEN
    RETURN;
  END IF;

  SELECT
    e.event_ticker,
    e.title AS event_title,
    e.sub_title AS event_sub_title,
    e.category AS event_category,
    e.strike_period,
    MIN(m.open_time) AS open_time,
    MAX(m.close_time) AS close_time,
    COUNT(*) AS market_count,
    COUNT(m.close_time) AS closed_market_count,
    SUM(mt.volume) AS volume,
    COALESCE(BOOL_OR(mt.ticker IS NOT NULL), false) AS has_tick,
    COALESCE(BOOL_OR(LOWER(am.status) IN ('open', 'active')), false) AS has_open_status,
    COALESCE(BOOL_OR(LOWER(am.status) = 'paused'), false) AS has_paused_status,
    CASE
      WHEN COALESCE(BOOL_OR(LOWER(am.status) IN ('open', 'active')), false)
        AND COALESCE(BOOL_OR(mt.ticker IS NOT NULL), false) THEN 'open'
      WHEN COALESCE(BOOL_OR(LOWER(am.status) = 'paused'), false)
        AND COALESCE(BOOL_OR(mt.ticker IS NOT NULL), false) THEN 'paused'
      WHEN MAX(m.close_time) IS NOT NULL AND MAX(m.close_time) <= NOW() THEN 'closed'
      WHEN MIN(m.open_time) IS NOT NULL AND MIN(m.open_time) > NOW() THEN 'scheduled'
      WHEN (MIN(m.open_time) IS NULL OR MIN(m.open_time) <= NOW())
        AND (MAX(m.close_time) IS NULL OR MAX(m.close_time) > NOW())
        AND COALESCE(BOOL_OR(mt.ticker IS NOT NULL), false) THEN 'open'
      WHEN (MIN(m.open_time) IS NULL OR MIN(m.open_time) <= NOW())
        AND (MAX(m.close_time) IS NULL OR MAX(m.close_time) > NOW())
        AND NOT COALESCE(BOOL_OR(mt.ticker IS NOT NULL), false) THEN 'scheduled'
      ELSE 'inactive'
    END AS status,
    (
      (MIN(m.open_time) IS NULL OR MIN(m.open_time) <= NOW())
      AND (MAX(m.close_time) IS NULL OR MAX(m.close_time) > NOW())
      AND COALESCE(BOOL_OR(mt.ticker IS NOT NULL), false)
    ) AS is_active,
    (
      (MIN(m.open_time) IS NOT NULL AND MIN(m.open_time) > NOW()
        AND (MAX(m.close_time) IS NULL OR MAX(m.close_time) > NOW()))
      OR ((MIN(m.open_time) IS NULL OR MIN(m.open_time) <= NOW())
        AND (MAX(m.close_time) IS NULL OR MAX(m.close_time) > NOW())
        AND NOT COALESCE(BOOL_OR(mt.ticker IS NOT NULL), false))
    ) AS is_scheduled,
    (
      COUNT(m.close_time) = COUNT(*)
      AND MAX(m.close_time) IS NOT NULL
      AND MAX(m.close_time) <= NOW()
    ) AS is_closed,
    p.predicted_yes_prob AS agent_yes_prob,
    p.confidence AS agent_confidence,
    p.created_at AS agent_prediction_ts,
    p.market_ticker AS agent_market_ticker
  INTO v_row
  FROM events e
  JOIN markets m ON m.event_ticker = e.event_ticker
  LEFT JOIN active_markets am ON am.ticker = m.ticker
  LEFT JOIN market_ticks_latest mt ON mt.ticker = m.ticker
  LEFT JOIN LATERAL (
    SELECT
      p.predicted_yes_prob,
      p.confidence,
      p.created_at,
      p.market_ticker
    FROM market_predictions p
    WHERE p.event_ticker = e.event_ticker
    ORDER BY p.created_at DESC, p.id DESC
    LIMIT 1
  ) p ON TRUE
  WHERE e.event_ticker = p_event_ticker
  GROUP BY
    e.event_ticker,
    e.title,
    e.sub_title,
    e.category,
    e.strike_period,
    p.predicted_yes_prob,
    p.confidence,
    p.created_at,
    p.market_ticker;

  IF NOT FOUND THEN
    DELETE FROM portal_event_rollup WHERE event_ticker = p_event_ticker;
    RETURN;
  END IF;

  INSERT INTO portal_event_rollup (
    event_ticker,
    event_title,
    event_sub_title,
    event_category,
    strike_period,
    open_time,
    close_time,
    market_count,
    closed_market_count,
    volume,
    has_tick,
    has_open_status,
    has_paused_status,
    status,
    is_active,
    is_scheduled,
    is_closed,
    agent_yes_prob,
    agent_confidence,
    agent_prediction_ts,
    agent_market_ticker,
    updated_at
  )
  VALUES (
    v_row.event_ticker,
    v_row.event_title,
    v_row.event_sub_title,
    v_row.event_category,
    v_row.strike_period,
    v_row.open_time,
    v_row.close_time,
    v_row.market_count,
    v_row.closed_market_count,
    v_row.volume,
    v_row.has_tick,
    v_row.has_open_status,
    v_row.has_paused_status,
    v_row.status,
    v_row.is_active,
    v_row.is_scheduled,
    v_row.is_closed,
    v_row.agent_yes_prob,
    v_row.agent_confidence,
    v_row.agent_prediction_ts,
    v_row.agent_market_ticker,
    NOW()
  )
  ON CONFLICT (event_ticker) DO UPDATE SET
    event_title = EXCLUDED.event_title,
    event_sub_title = EXCLUDED.event_sub_title,
    event_category = EXCLUDED.event_category,
    strike_period = EXCLUDED.strike_period,
    open_time = EXCLUDED.open_time,
    close_time = EXCLUDED.close_time,
    market_count = EXCLUDED.market_count,
    closed_market_count = EXCLUDED.closed_market_count,
    volume = EXCLUDED.volume,
    has_tick = EXCLUDED.has_tick,
    has_open_status = EXCLUDED.has_open_status,
    has_paused_status = EXCLUDED.has_paused_status,
    status = EXCLUDED.status,
    is_active = EXCLUDED.is_active,
    is_scheduled = EXCLUDED.is_scheduled,
    is_closed = EXCLUDED.is_closed,
    agent_yes_prob = EXCLUDED.agent_yes_prob,
    agent_confidence = EXCLUDED.agent_confidence,
    agent_prediction_ts = EXCLUDED.agent_prediction_ts,
    agent_market_ticker = EXCLUDED.agent_market_ticker,
    updated_at = NOW();
END $$;

CREATE OR REPLACE FUNCTION portal_backfill_event_rollups()
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
DECLARE
  v_count INTEGER := 0;
BEGIN
  TRUNCATE portal_event_rollup;

  INSERT INTO portal_event_rollup (
    event_ticker,
    event_title,
    event_sub_title,
    event_category,
    strike_period,
    open_time,
    close_time,
    market_count,
    closed_market_count,
    volume,
    has_tick,
    has_open_status,
    has_paused_status,
    status,
    is_active,
    is_scheduled,
    is_closed,
    agent_yes_prob,
    agent_confidence,
    agent_prediction_ts,
    agent_market_ticker,
    updated_at
  )
  SELECT
    e.event_ticker,
    e.title AS event_title,
    e.sub_title AS event_sub_title,
    e.category AS event_category,
    e.strike_period,
    MIN(m.open_time) AS open_time,
    MAX(m.close_time) AS close_time,
    COUNT(*) AS market_count,
    COUNT(m.close_time) AS closed_market_count,
    SUM(mt.volume) AS volume,
    COALESCE(BOOL_OR(mt.ticker IS NOT NULL), false) AS has_tick,
    COALESCE(BOOL_OR(LOWER(am.status) IN ('open', 'active')), false) AS has_open_status,
    COALESCE(BOOL_OR(LOWER(am.status) = 'paused'), false) AS has_paused_status,
    CASE
      WHEN COALESCE(BOOL_OR(LOWER(am.status) IN ('open', 'active')), false)
        AND COALESCE(BOOL_OR(mt.ticker IS NOT NULL), false) THEN 'open'
      WHEN COALESCE(BOOL_OR(LOWER(am.status) = 'paused'), false)
        AND COALESCE(BOOL_OR(mt.ticker IS NOT NULL), false) THEN 'paused'
      WHEN MAX(m.close_time) IS NOT NULL AND MAX(m.close_time) <= NOW() THEN 'closed'
      WHEN MIN(m.open_time) IS NOT NULL AND MIN(m.open_time) > NOW() THEN 'scheduled'
      WHEN (MIN(m.open_time) IS NULL OR MIN(m.open_time) <= NOW())
        AND (MAX(m.close_time) IS NULL OR MAX(m.close_time) > NOW())
        AND COALESCE(BOOL_OR(mt.ticker IS NOT NULL), false) THEN 'open'
      WHEN (MIN(m.open_time) IS NULL OR MIN(m.open_time) <= NOW())
        AND (MAX(m.close_time) IS NULL OR MAX(m.close_time) > NOW())
        AND NOT COALESCE(BOOL_OR(mt.ticker IS NOT NULL), false) THEN 'scheduled'
      ELSE 'inactive'
    END AS status,
    (
      (MIN(m.open_time) IS NULL OR MIN(m.open_time) <= NOW())
      AND (MAX(m.close_time) IS NULL OR MAX(m.close_time) > NOW())
      AND COALESCE(BOOL_OR(mt.ticker IS NOT NULL), false)
    ) AS is_active,
    (
      (MIN(m.open_time) IS NOT NULL AND MIN(m.open_time) > NOW()
        AND (MAX(m.close_time) IS NULL OR MAX(m.close_time) > NOW()))
      OR ((MIN(m.open_time) IS NULL OR MIN(m.open_time) <= NOW())
        AND (MAX(m.close_time) IS NULL OR MAX(m.close_time) > NOW())
        AND NOT COALESCE(BOOL_OR(mt.ticker IS NOT NULL), false))
    ) AS is_scheduled,
    (
      COUNT(m.close_time) = COUNT(*)
      AND MAX(m.close_time) IS NOT NULL
      AND MAX(m.close_time) <= NOW()
    ) AS is_closed,
    p.predicted_yes_prob AS agent_yes_prob,
    p.confidence AS agent_confidence,
    p.created_at AS agent_prediction_ts,
    p.market_ticker AS agent_market_ticker,
    NOW() AS updated_at
  FROM events e
  JOIN markets m ON m.event_ticker = e.event_ticker
  LEFT JOIN active_markets am ON am.ticker = m.ticker
  LEFT JOIN market_ticks_latest mt ON mt.ticker = m.ticker
  LEFT JOIN LATERAL (
    SELECT
      p.predicted_yes_prob,
      p.confidence,
      p.created_at,
      p.market_ticker
    FROM market_predictions p
    WHERE p.event_ticker = e.event_ticker
    ORDER BY p.created_at DESC, p.id DESC
    LIMIT 1
  ) p ON TRUE
  GROUP BY
    e.event_ticker,
    e.title,
    e.sub_title,
    e.category,
    e.strike_period,
    p.predicted_yes_prob,
    p.confidence,
    p.created_at,
    p.market_ticker;

  GET DIAGNOSTICS v_count = ROW_COUNT;
  RETURN v_count;
END $$;

CREATE OR REPLACE FUNCTION portal_rollup_refresh_trigger()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
DECLARE
  v_event_tickers TEXT[] := ARRAY[]::TEXT[];
  v_event_ticker TEXT;
  v_market_ticker TEXT;
  v_payload JSONB;
BEGIN
  IF TG_OP IN ('INSERT', 'UPDATE') THEN
    v_payload := to_jsonb(NEW);
    v_event_ticker := NULLIF(BTRIM(v_payload->>'event_ticker'), '');
    v_market_ticker := NULLIF(BTRIM(v_payload->>'ticker'), '');
    IF v_event_ticker IS NULL AND v_market_ticker IS NOT NULL THEN
      SELECT event_ticker INTO v_event_ticker FROM markets WHERE ticker = v_market_ticker;
    END IF;
    IF v_event_ticker IS NOT NULL THEN
      v_event_tickers := array_append(v_event_tickers, v_event_ticker);
    END IF;
  END IF;

  IF TG_OP IN ('UPDATE', 'DELETE') THEN
    v_payload := to_jsonb(OLD);
    v_event_ticker := NULLIF(BTRIM(v_payload->>'event_ticker'), '');
    v_market_ticker := NULLIF(BTRIM(v_payload->>'ticker'), '');
    IF v_event_ticker IS NULL AND v_market_ticker IS NOT NULL THEN
      SELECT event_ticker INTO v_event_ticker FROM markets WHERE ticker = v_market_ticker;
    END IF;
    IF v_event_ticker IS NOT NULL THEN
      v_event_tickers := array_append(v_event_tickers, v_event_ticker);
    END IF;
  END IF;

  FOR v_event_ticker IN SELECT DISTINCT unnest(v_event_tickers) ORDER BY 1 LOOP
    PERFORM portal_refresh_event_rollup(v_event_ticker);
  END LOOP;
  RETURN NULL;
END $$;

DROP TRIGGER IF EXISTS trg_portal_rollup_markets ON markets;
CREATE TRIGGER trg_portal_rollup_markets
AFTER INSERT OR UPDATE OR DELETE ON markets
FOR EACH ROW EXECUTE FUNCTION portal_rollup_refresh_trigger();

DROP TRIGGER IF EXISTS trg_portal_rollup_active_markets ON active_markets;
CREATE TRIGGER trg_portal_rollup_active_markets
AFTER INSERT OR UPDATE OR DELETE ON active_markets
FOR EACH ROW EXECUTE FUNCTION portal_rollup_refresh_trigger();

DROP TRIGGER IF EXISTS trg_portal_rollup_market_ticks ON market_ticks;
DROP TRIGGER IF EXISTS trg_portal_rollup_market_ticks ON market_ticks_latest;
CREATE TRIGGER trg_portal_rollup_market_ticks
AFTER INSERT OR UPDATE OR DELETE ON market_ticks_latest
FOR EACH ROW EXECUTE FUNCTION portal_rollup_refresh_trigger();

DROP TRIGGER IF EXISTS trg_portal_rollup_market_predictions ON market_predictions;
CREATE TRIGGER trg_portal_rollup_market_predictions
AFTER INSERT OR UPDATE OR DELETE ON market_predictions
FOR EACH ROW EXECUTE FUNCTION portal_rollup_refresh_trigger();

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM portal_event_rollup LIMIT 1)
     OR EXISTS (SELECT 1 FROM portal_event_rollup WHERE status IS NULL LIMIT 1) THEN
    PERFORM portal_backfill_event_rollups();
  END IF;
END $$;

CREATE OR REPLACE FUNCTION portal_snapshot_json(
  p_limit INTEGER,
  p_search TEXT,
  p_categories TEXT[],
  p_strike_period TEXT,
  p_close_window_hours DOUBLE PRECISION,
  p_status TEXT,
  p_sort TEXT,
  p_order TEXT,
  p_queue_lock_timeout_sec INTEGER,
  p_active_cursor_value TEXT,
  p_active_cursor_ticker TEXT,
  p_scheduled_cursor_value TEXT,
  p_scheduled_cursor_ticker TEXT,
  p_closed_cursor_value TEXT,
  p_closed_cursor_ticker TEXT,
  p_include_health BOOLEAN DEFAULT TRUE,
  p_use_status BOOLEAN DEFAULT TRUE
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
  v_limit INTEGER := GREATEST(COALESCE(p_limit, 200), 1);
  v_search TEXT := NULLIF(BTRIM(p_search), '');
  v_pattern TEXT := NULL;
  v_categories TEXT[] := p_categories;
  v_strike TEXT := NULLIF(LOWER(BTRIM(p_strike_period)), '');
  v_status TEXT := NULLIF(LOWER(BTRIM(p_status)), '');
  v_order TEXT := LOWER(COALESCE(p_order, ''));
  v_sort TEXT := LOWER(COALESCE(p_sort, ''));
  v_queue_lock_timeout_sec INTEGER := GREATEST(COALESCE(p_queue_lock_timeout_sec, 900), 10);
  v_active_cursor_value TEXT := NULLIF(BTRIM(p_active_cursor_value), '');
  v_active_cursor_ticker TEXT := NULLIF(BTRIM(p_active_cursor_ticker), '');
  v_scheduled_cursor_value TEXT := NULLIF(BTRIM(p_scheduled_cursor_value), '');
  v_scheduled_cursor_ticker TEXT := NULLIF(BTRIM(p_scheduled_cursor_ticker), '');
  v_closed_cursor_value TEXT := NULLIF(BTRIM(p_closed_cursor_value), '');
  v_closed_cursor_ticker TEXT := NULLIF(BTRIM(p_closed_cursor_ticker), '');
  v_include_health BOOLEAN := COALESCE(p_include_health, TRUE);
  v_use_status BOOLEAN := COALESCE(p_use_status, TRUE);
  active_sort TEXT;
  scheduled_sort TEXT;
  active_order TEXT;
  scheduled_order TEXT;
  closed_order TEXT;
  active_comp TEXT;
  scheduled_comp TEXT;
  closed_comp TEXT;
  active_cursor_value_expr TEXT;
  scheduled_cursor_value_expr TEXT;
  closed_cursor_value_expr TEXT;
  active_cursor_clause TEXT := '';
  scheduled_cursor_clause TEXT := '';
  closed_cursor_clause TEXT := '';
  status_expr TEXT;
  active_expr TEXT;
  scheduled_expr TEXT;
  closed_expr TEXT;
  sql TEXT;
  result JSONB;
BEGIN
  IF v_categories IS NOT NULL THEN
    v_categories := ARRAY(
      SELECT LOWER(TRIM(token))
      FROM UNNEST(v_categories) AS token
      WHERE token IS NOT NULL AND BTRIM(token) <> ''
    );
    IF COALESCE(array_length(v_categories, 1), 0) = 0 THEN
      v_categories := NULL;
    END IF;
  END IF;

  IF v_search IS NOT NULL THEN
    v_pattern := '%' || v_search || '%';
  END IF;

  IF v_order NOT IN ('asc', 'desc') THEN
    v_order := 'asc';
  END IF;
  IF v_sort NOT IN ('close_time', 'open_time', 'title', 'category', 'strike_period', 'market_count') THEN
    v_sort := NULL;
  END IF;

  IF v_sort = 'title' THEN
    active_sort := 'event_title';
    scheduled_sort := 'event_title';
  ELSIF v_sort = 'category' THEN
    active_sort := 'event_category';
    scheduled_sort := 'event_category';
  ELSE
    active_sort := COALESCE(v_sort, 'close_time');
    scheduled_sort := COALESCE(v_sort, 'open_time');
  END IF;

  active_order := format('%I %s NULLS LAST, event_ticker %s', active_sort, v_order, v_order);
  scheduled_order := format('%I %s NULLS LAST, event_ticker %s', scheduled_sort, v_order, v_order);
  closed_order := 'close_time DESC NULLS LAST, event_ticker DESC';
  active_comp := CASE WHEN v_order = 'desc' THEN '<' ELSE '>' END;
  scheduled_comp := CASE WHEN v_order = 'desc' THEN '<' ELSE '>' END;
  closed_comp := '<';

  IF active_sort IN ('close_time', 'open_time') THEN
    active_cursor_value_expr := '$8::timestamptz';
  ELSIF active_sort = 'market_count' THEN
    active_cursor_value_expr := '$8::integer';
  ELSE
    active_cursor_value_expr := '$8::text';
  END IF;

  IF scheduled_sort IN ('close_time', 'open_time') THEN
    scheduled_cursor_value_expr := '$10::timestamptz';
  ELSIF scheduled_sort = 'market_count' THEN
    scheduled_cursor_value_expr := '$10::integer';
  ELSE
    scheduled_cursor_value_expr := '$10::text';
  END IF;

  closed_cursor_value_expr := '$12::timestamptz';

  IF v_active_cursor_ticker IS NOT NULL THEN
    IF v_active_cursor_value IS NULL THEN
      active_cursor_clause := format(
        'AND (%1$I IS NULL AND event_ticker %2$s $9)',
        active_sort,
        active_comp
      );
    ELSE
      active_cursor_clause := format(
        'AND ((%1$I %2$s %3$s) OR (%1$I = %3$s AND event_ticker %2$s $9) OR %1$I IS NULL)',
        active_sort,
        active_comp,
        active_cursor_value_expr
      );
    END IF;
  END IF;

  IF v_scheduled_cursor_ticker IS NOT NULL THEN
    IF v_scheduled_cursor_value IS NULL THEN
      scheduled_cursor_clause := format(
        'AND (%1$I IS NULL AND event_ticker %2$s $11)',
        scheduled_sort,
        scheduled_comp
      );
    ELSE
      scheduled_cursor_clause := format(
        'AND ((%1$I %2$s %3$s) OR (%1$I = %3$s AND event_ticker %2$s $11) OR %1$I IS NULL)',
        scheduled_sort,
        scheduled_comp,
        scheduled_cursor_value_expr
      );
    END IF;
  END IF;

  IF v_closed_cursor_ticker IS NOT NULL THEN
    IF v_closed_cursor_value IS NULL THEN
      closed_cursor_clause := format(
        'AND (close_time IS NULL AND event_ticker %s $13)',
        closed_comp
      );
    ELSE
      closed_cursor_clause := format(
        'AND ((close_time %s %s) OR (close_time = %s AND event_ticker %s $13))',
        closed_comp,
        closed_cursor_value_expr,
        closed_cursor_value_expr,
        closed_comp
      );
    END IF;
  END IF;

  IF v_use_status THEN
    status_expr := 'status';
    active_expr := 'is_active';
    scheduled_expr := 'is_scheduled';
    closed_expr := 'is_closed';
  ELSE
    status_expr := (
      'CASE '
      'WHEN has_open_status AND has_tick THEN ''open'' '
      'WHEN has_paused_status AND has_tick THEN ''paused'' '
      'WHEN close_time IS NOT NULL AND close_time <= NOW() THEN ''closed'' '
      'WHEN open_time IS NOT NULL AND open_time > NOW() THEN ''scheduled'' '
      'WHEN (open_time IS NULL OR open_time <= NOW()) '
      ' AND (close_time IS NULL OR close_time > NOW()) '
      ' AND has_tick THEN ''open'' '
      'WHEN (open_time IS NULL OR open_time <= NOW()) '
      ' AND (close_time IS NULL OR close_time > NOW()) '
      ' AND NOT has_tick THEN ''scheduled'' '
      'ELSE ''inactive'' '
      'END'
    );
    active_expr := (
      '(open_time IS NULL OR open_time <= NOW()) '
      'AND (close_time IS NULL OR close_time > NOW()) '
      'AND has_tick'
    );
    scheduled_expr := (
      '((open_time IS NOT NULL AND open_time > NOW() '
      '  AND (close_time IS NULL OR close_time > NOW())) '
      ' OR ((open_time IS NULL OR open_time <= NOW()) '
      '  AND (close_time IS NULL OR close_time > NOW()) '
      '  AND NOT has_tick))'
    );
    closed_expr := (
      'closed_market_count = market_count '
      'AND close_time IS NOT NULL '
      'AND close_time <= NOW()'
    );
  END IF;

  IF v_include_health THEN
    sql := format($sql$
    WITH base AS (
      SELECT
        event_ticker,
        event_title,
        event_sub_title,
        event_category,
        strike_period,
        open_time,
        close_time,
        market_count,
        closed_market_count,
        volume,
        has_tick,
        has_open_status,
        has_paused_status,
        agent_yes_prob,
        agent_confidence,
        agent_prediction_ts,
        agent_market_ticker,
        is_active,
        is_scheduled,
        is_closed,
        %s AS status
      FROM portal_event_rollup
      WHERE TRUE
        AND ($2 IS NULL OR (
          search_text ILIKE $2
        ))
        AND ($3 IS NULL OR LOWER(COALESCE(event_category, '')) = ANY($3))
        AND ($4 IS NULL OR LOWER(strike_period) = $4)
    ),
    filtered AS (
      SELECT * FROM base
      WHERE $5 IS NULL OR status = $5
    ),
    active_base AS (
      SELECT * FROM filtered
      WHERE %s
        AND ($6 IS NULL OR (
          close_time IS NOT NULL
          AND close_time <= NOW() + ($6 * INTERVAL '1 hour')
        ))
    ),
    scheduled_base AS (
      SELECT * FROM filtered
      WHERE %s
      AND ($6 IS NULL OR (
        close_time IS NOT NULL
        AND close_time <= NOW() + ($6 * INTERVAL '1 hour')
      ))
    ),
    closed_base AS (
      SELECT * FROM filtered
      WHERE %s
        AND ($6 IS NULL OR (
          close_time IS NOT NULL
          AND close_time >= NOW() - ($6 * INTERVAL '1 hour')
        ))
    ),
    active AS (
      SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY %s) AS ord
      FROM active_base
      WHERE TRUE
        %s
      ORDER BY %s
      LIMIT $1
    ),
    scheduled AS (
      SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY %s) AS ord
      FROM scheduled_base
      WHERE TRUE
        %s
      ORDER BY %s
      LIMIT $1
    ),
    closed AS (
      SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY %s) AS ord
      FROM closed_base
      WHERE TRUE
        %s
      ORDER BY %s
      LIMIT $1
    ),
    counts AS (
      SELECT
        (SELECT COUNT(*) FROM active_base) AS active_total,
        (SELECT COUNT(*) FROM scheduled_base) AS scheduled_total,
        (SELECT COUNT(*) FROM closed_base) AS closed_total
    ),
    active_categories AS (
      SELECT DISTINCT event_category
      FROM (
        SELECT
          event_category,
          open_time,
          close_time,
          has_tick,
          is_active,
          is_scheduled,
          is_closed,
          %s AS status
        FROM portal_event_rollup
        WHERE TRUE
          AND ($2 IS NULL OR (
            search_text ILIKE $2
          ))
          AND ($4 IS NULL OR LOWER(strike_period) = $4)
      ) active_scope
      WHERE %s
        AND ($5 IS NULL OR status = $5)
        AND ($6 IS NULL OR (
          close_time IS NOT NULL
          AND close_time <= NOW() + ($6 * INTERVAL '1 hour')
        ))
        AND event_category IS NOT NULL
        AND event_category <> ''
      ORDER BY event_category
    ),
    strike_periods AS (
      SELECT strike_period
      FROM events
      WHERE strike_period IS NOT NULL AND strike_period <> ''
      GROUP BY strike_period
      ORDER BY strike_period
    ),
    state_rows AS (
      SELECT key, value, updated_at
      FROM ingest_state
      WHERE key IN (
        'last_discovery_ts',
        'last_min_close_ts',
        'last_prediction_ts',
        'rag_24h_calls',
        'last_tick_ts',
        'last_ws_tick_ts',
        'ws_heartbeat'
      )
    ),
    state_json AS (
      SELECT COALESCE(
        jsonb_object_agg(
          key,
          jsonb_build_object('value', value, 'updated_at', updated_at)
        ),
        '{}'::jsonb
      ) AS value
      FROM state_rows
    ),
    queue_stats AS (
      SELECT jsonb_build_object(
        'pending', COUNT(*) FILTER (
          WHERE status = 'pending' AND available_at <= NOW()
        ),
        'running', COUNT(*) FILTER (
          WHERE status = 'running'
            AND locked_at >= NOW() - ($7 * INTERVAL '1 second')
        ),
        'failed', COUNT(*) FILTER (WHERE status = 'failed'),
        'workers', COUNT(DISTINCT locked_by) FILTER (
          WHERE status = 'running'
            AND locked_by IS NOT NULL
            AND locked_at >= NOW() - ($7 * INTERVAL '1 second')
        )
      ) AS value
      FROM work_queue
    ),
    latest_prediction AS (
      SELECT MAX(ts) AS value
      FROM (
        SELECT MAX(run_ts) AS ts FROM prediction_runs
        UNION ALL
        SELECT MAX(created_at) AS ts FROM market_predictions
      ) raw_ts
    ),
    health_raw AS (
      SELECT jsonb_build_object(
        'state_rows', (SELECT value FROM state_json),
        'latest_tick_ts', (SELECT MAX(ts) FROM market_ticks_latest),
        'latest_prediction_ts', (SELECT value FROM latest_prediction),
        'queue', (SELECT value FROM queue_stats)
      ) AS value
    )
    SELECT jsonb_build_object(
      'active_rows',
      COALESCE(
        (
          SELECT jsonb_agg(row ORDER BY ord)
          FROM (
            SELECT jsonb_build_object(
              'event_ticker', event_ticker,
              'event_title', event_title,
              'event_category', event_category,
              'strike_period', strike_period,
              'open_time', open_time,
              'close_time', close_time,
              'market_count', market_count,
              'volume', volume,
              'agent_yes_prob', agent_yes_prob,
              'agent_confidence', agent_confidence,
              'agent_prediction_ts', agent_prediction_ts,
              'agent_market_ticker', agent_market_ticker
            ) AS row,
            ord
            FROM active
          ) rows
        ),
        '[]'::jsonb
      ),
      'scheduled_rows',
      COALESCE(
        (
          SELECT jsonb_agg(row ORDER BY ord)
          FROM (
            SELECT jsonb_build_object(
              'event_ticker', event_ticker,
              'event_title', event_title,
              'event_category', event_category,
              'strike_period', strike_period,
              'open_time', open_time,
              'close_time', close_time,
              'market_count', market_count,
              'volume', volume
            ) AS row,
            ord
            FROM scheduled
          ) rows
        ),
        '[]'::jsonb
      ),
      'closed_rows',
      COALESCE(
        (
          SELECT jsonb_agg(row ORDER BY ord)
          FROM (
            SELECT jsonb_build_object(
              'event_ticker', event_ticker,
              'event_title', event_title,
              'event_category', event_category,
              'strike_period', strike_period,
              'open_time', open_time,
              'close_time', close_time,
              'market_count', market_count,
              'volume', volume
            ) AS row,
            ord
            FROM closed
          ) rows
        ),
        '[]'::jsonb
      ),
      'active_total', (SELECT active_total FROM counts),
      'scheduled_total', (SELECT scheduled_total FROM counts),
      'closed_total', (SELECT closed_total FROM counts),
      'strike_periods',
      COALESCE(
        (
          SELECT jsonb_agg(strike_period ORDER BY strike_period)
          FROM strike_periods
        ),
        '[]'::jsonb
      ),
      'active_categories',
      COALESCE(
        (
          SELECT jsonb_agg(event_category ORDER BY event_category)
          FROM active_categories
        ),
        '[]'::jsonb
      ),
      'health_raw', (SELECT value FROM health_raw)
    )
    $sql$,
    status_expr,
    active_expr,
    scheduled_expr,
    closed_expr,
    active_order,
    active_cursor_clause,
    active_order,
    scheduled_order,
    scheduled_cursor_clause,
    scheduled_order,
    closed_order,
    closed_cursor_clause,
    closed_order,
    status_expr,
    active_expr
  );
  ELSE
    sql := format($sql$
      WITH base AS (
        SELECT
          event_ticker,
          event_title,
          event_sub_title,
          event_category,
          strike_period,
          open_time,
          close_time,
          market_count,
          closed_market_count,
          volume,
          has_tick,
          has_open_status,
          has_paused_status,
          agent_yes_prob,
          agent_confidence,
          agent_prediction_ts,
          agent_market_ticker,
          is_active,
          is_scheduled,
          is_closed,
          %s AS status
        FROM portal_event_rollup
        WHERE TRUE
          AND ($2 IS NULL OR (
            search_text ILIKE $2
          ))
          AND ($3 IS NULL OR LOWER(COALESCE(event_category, '')) = ANY($3))
          AND ($4 IS NULL OR LOWER(strike_period) = $4)
      ),
      filtered AS (
        SELECT * FROM base
        WHERE $5 IS NULL OR status = $5
      ),
      active_base AS (
        SELECT * FROM filtered
        WHERE %s
        AND ($6 IS NULL OR (
          close_time IS NOT NULL
          AND close_time <= NOW() + ($6 * INTERVAL '1 hour')
        ))
      ),
      scheduled_base AS (
        SELECT * FROM filtered
        WHERE %s
        AND ($6 IS NULL OR (
          close_time IS NOT NULL
          AND close_time <= NOW() + ($6 * INTERVAL '1 hour')
        ))
      ),
      closed_base AS (
        SELECT * FROM filtered
        WHERE %s
        AND ($6 IS NULL OR (
          close_time IS NOT NULL
          AND close_time >= NOW() - ($6 * INTERVAL '1 hour')
        ))
      ),
      active AS (
        SELECT
          *,
          ROW_NUMBER() OVER (ORDER BY %s) AS ord
        FROM active_base
        WHERE TRUE
          %s
        ORDER BY %s
        LIMIT $1
      ),
      scheduled AS (
        SELECT
          *,
          ROW_NUMBER() OVER (ORDER BY %s) AS ord
        FROM scheduled_base
        WHERE TRUE
          %s
        ORDER BY %s
        LIMIT $1
      ),
      closed AS (
        SELECT
          *,
          ROW_NUMBER() OVER (ORDER BY %s) AS ord
        FROM closed_base
        WHERE TRUE
          %s
        ORDER BY %s
        LIMIT $1
      ),
      counts AS (
        SELECT
          (SELECT COUNT(*) FROM active_base) AS active_total,
          (SELECT COUNT(*) FROM scheduled_base) AS scheduled_total,
          (SELECT COUNT(*) FROM closed_base) AS closed_total
      ),
      active_categories AS (
        SELECT DISTINCT event_category
        FROM (
          SELECT
            event_category,
            open_time,
            close_time,
            has_tick,
            is_active,
            is_scheduled,
            is_closed,
            %s AS status
          FROM portal_event_rollup
          WHERE TRUE
            AND ($2 IS NULL OR (
              search_text ILIKE $2
            ))
            AND ($4 IS NULL OR LOWER(strike_period) = $4)
        ) active_scope
        WHERE %s
          AND ($5 IS NULL OR status = $5)
          AND ($6 IS NULL OR (
            close_time IS NOT NULL
            AND close_time <= NOW() + ($6 * INTERVAL '1 hour')
          ))
          AND event_category IS NOT NULL
          AND event_category <> ''
        ORDER BY event_category
      ),
      strike_periods AS (
        SELECT strike_period
        FROM events
        WHERE strike_period IS NOT NULL AND strike_period <> ''
        GROUP BY strike_period
        ORDER BY strike_period
      )
      SELECT jsonb_build_object(
        'active_rows',
        COALESCE(
          (
            SELECT jsonb_agg(row ORDER BY ord)
            FROM (
              SELECT jsonb_build_object(
                'event_ticker', event_ticker,
                'event_title', event_title,
                'event_category', event_category,
                'strike_period', strike_period,
                'open_time', open_time,
                'close_time', close_time,
                'market_count', market_count,
                'volume', volume,
                'agent_yes_prob', agent_yes_prob,
                'agent_confidence', agent_confidence,
                'agent_prediction_ts', agent_prediction_ts,
                'agent_market_ticker', agent_market_ticker
              ) AS row,
              ord
              FROM active
            ) rows
          ),
          '[]'::jsonb
        ),
        'scheduled_rows',
        COALESCE(
          (
            SELECT jsonb_agg(row ORDER BY ord)
            FROM (
              SELECT jsonb_build_object(
                'event_ticker', event_ticker,
                'event_title', event_title,
                'event_category', event_category,
                'strike_period', strike_period,
                'open_time', open_time,
                'close_time', close_time,
                'market_count', market_count,
                'volume', volume
              ) AS row,
              ord
              FROM scheduled
            ) rows
          ),
          '[]'::jsonb
        ),
        'closed_rows',
        COALESCE(
          (
            SELECT jsonb_agg(row ORDER BY ord)
            FROM (
              SELECT jsonb_build_object(
                'event_ticker', event_ticker,
                'event_title', event_title,
                'event_category', event_category,
                'strike_period', strike_period,
                'open_time', open_time,
                'close_time', close_time,
                'market_count', market_count,
                'volume', volume
              ) AS row,
              ord
              FROM closed
            ) rows
          ),
          '[]'::jsonb
        ),
        'active_total', (SELECT active_total FROM counts),
        'scheduled_total', (SELECT scheduled_total FROM counts),
        'closed_total', (SELECT closed_total FROM counts),
        'strike_periods',
        COALESCE(
          (
            SELECT jsonb_agg(strike_period ORDER BY strike_period)
            FROM strike_periods
          ),
          '[]'::jsonb
        ),
        'active_categories',
        COALESCE(
          (
            SELECT jsonb_agg(event_category ORDER BY event_category)
            FROM active_categories
          ),
          '[]'::jsonb
        ),
        'health_raw', NULL
      )
      $sql$,
      status_expr,
      active_expr,
      scheduled_expr,
      closed_expr,
      active_order,
      active_cursor_clause,
      active_order,
      scheduled_order,
      scheduled_cursor_clause,
      scheduled_order,
      closed_order,
      closed_cursor_clause,
      closed_order,
      status_expr,
      active_expr
    );
  END IF;

  EXECUTE sql INTO result
    USING v_limit,
      v_pattern,
      v_categories,
      v_strike,
      v_status,
      p_close_window_hours,
      v_queue_lock_timeout_sec,
      v_active_cursor_value,
      v_active_cursor_ticker,
      v_scheduled_cursor_value,
      v_scheduled_cursor_ticker,
      v_closed_cursor_value,
      v_closed_cursor_ticker;
  RETURN result;
END;
$$;
