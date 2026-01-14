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

CREATE INDEX IF NOT EXISTS idx_events_strike_period ON events(strike_period);

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
CREATE INDEX IF NOT EXISTS idx_markets_open_time ON markets(open_time);
CREATE INDEX IF NOT EXISTS idx_markets_close_time ON markets(close_time);

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

-- ========= Schema versioning =========

CREATE TABLE IF NOT EXISTS schema_version (
  version INTEGER PRIMARY KEY,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO schema_version(version)
VALUES (1)
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

-- ========= Portal snapshot helpers =========

CREATE MATERIALIZED VIEW IF NOT EXISTS portal_event_rollup AS
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
  SUM(tv.volume) AS volume,
  COALESCE(BOOL_OR(mt.has_tick), false) AS has_tick,
  COALESCE(BOOL_OR(LOWER(am.status) IN ('open', 'active')), false) AS has_open_status,
  COALESCE(BOOL_OR(LOWER(am.status) = 'paused'), false) AS has_paused_status,
  p.predicted_yes_prob AS agent_yes_prob,
  p.confidence AS agent_confidence,
  p.created_at AS agent_prediction_ts,
  p.market_ticker AS agent_market_ticker
FROM events e
JOIN markets m ON m.event_ticker = e.event_ticker
LEFT JOIN active_markets am ON am.ticker = m.ticker
LEFT JOIN LATERAL (
  SELECT TRUE AS has_tick
  FROM market_ticks t
  WHERE t.ticker = m.ticker
  LIMIT 1
) mt ON TRUE
LEFT JOIN LATERAL (
  SELECT t.volume
  FROM market_ticks t
  WHERE t.ticker = m.ticker
  ORDER BY t.ts DESC, t.id DESC
  LIMIT 1
) tv ON TRUE
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

CREATE UNIQUE INDEX IF NOT EXISTS idx_portal_event_rollup_ticker
  ON portal_event_rollup(event_ticker);

CREATE OR REPLACE FUNCTION portal_snapshot_json(
  p_limit INTEGER,
  p_search TEXT,
  p_categories TEXT[],
  p_strike_period TEXT,
  p_close_window_hours DOUBLE PRECISION,
  p_status TEXT,
  p_sort TEXT,
  p_order TEXT,
  p_queue_lock_timeout_sec INTEGER
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
  active_sort TEXT;
  scheduled_sort TEXT;
  active_order TEXT;
  scheduled_order TEXT;
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

  active_sort := COALESCE(v_sort, 'close_time');
  scheduled_sort := COALESCE(v_sort, 'open_time');
  active_order := format('%I %s NULLS LAST', active_sort, v_order);
  scheduled_order := format('%I %s NULLS LAST', scheduled_sort, v_order);

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
        CASE
          WHEN has_open_status AND has_tick THEN 'open'
          WHEN has_paused_status AND has_tick THEN 'paused'
          WHEN close_time IS NOT NULL AND close_time <= NOW() THEN 'closed'
          WHEN open_time IS NOT NULL AND open_time > NOW() THEN 'scheduled'
          WHEN (open_time IS NULL OR open_time <= NOW())
           AND (close_time IS NULL OR close_time > NOW())
           AND has_tick THEN 'open'
          WHEN (open_time IS NULL OR open_time <= NOW())
           AND (close_time IS NULL OR close_time > NOW())
           AND NOT has_tick THEN 'scheduled'
          ELSE 'inactive'
        END AS status
      FROM portal_event_rollup
      WHERE TRUE
        AND ($2 IS NULL OR (
          event_title ILIKE $2
          OR event_sub_title ILIKE $2
          OR event_ticker ILIKE $2
          OR event_category ILIKE $2
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
      WHERE (open_time IS NULL OR open_time <= NOW())
        AND (close_time IS NULL OR close_time > NOW())
        AND has_tick
        AND ($6 IS NULL OR (
          close_time IS NOT NULL
          AND close_time <= NOW() + ($6 * INTERVAL '1 hour')
        ))
    ),
    scheduled_base AS (
      SELECT * FROM filtered
      WHERE (
        (open_time IS NOT NULL AND open_time > NOW()
          AND (close_time IS NULL OR close_time > NOW()))
        OR ((open_time IS NULL OR open_time <= NOW())
          AND (close_time IS NULL OR close_time > NOW())
          AND NOT has_tick)
      )
      AND ($6 IS NULL OR (
        close_time IS NOT NULL
        AND close_time <= NOW() + ($6 * INTERVAL '1 hour')
      ))
    ),
    closed_base AS (
      SELECT * FROM filtered
      WHERE closed_market_count = market_count
        AND close_time IS NOT NULL
        AND close_time <= NOW()
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
      ORDER BY %s
      LIMIT $1
    ),
    scheduled AS (
      SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY %s) AS ord
      FROM scheduled_base
      ORDER BY %s
      LIMIT $1
    ),
    closed AS (
      SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY close_time DESC NULLS LAST) AS ord
      FROM closed_base
      ORDER BY close_time DESC NULLS LAST
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
          CASE
            WHEN has_open_status AND has_tick THEN 'open'
            WHEN has_paused_status AND has_tick THEN 'paused'
            WHEN close_time IS NOT NULL AND close_time <= NOW() THEN 'closed'
            WHEN open_time IS NOT NULL AND open_time > NOW() THEN 'scheduled'
            WHEN (open_time IS NULL OR open_time <= NOW())
             AND (close_time IS NULL OR close_time > NOW())
             AND has_tick THEN 'open'
            WHEN (open_time IS NULL OR open_time <= NOW())
             AND (close_time IS NULL OR close_time > NOW())
             AND NOT has_tick THEN 'scheduled'
            ELSE 'inactive'
          END AS status
        FROM portal_event_rollup
        WHERE TRUE
          AND ($2 IS NULL OR (
            event_title ILIKE $2
            OR event_sub_title ILIKE $2
            OR event_ticker ILIKE $2
            OR event_category ILIKE $2
          ))
          AND ($4 IS NULL OR LOWER(strike_period) = $4)
      ) active_scope
      WHERE (open_time IS NULL OR open_time <= NOW())
        AND (close_time IS NULL OR close_time > NOW())
        AND has_tick
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
        'latest_tick_ts', (SELECT MAX(ts) FROM market_ticks),
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
    active_order,
    active_order,
    scheduled_order,
    scheduled_order
  );

  EXECUTE sql INTO result
    USING v_limit,
      v_pattern,
      v_categories,
      v_strike,
      v_status,
      p_close_window_hours,
      v_queue_lock_timeout_sec;
  RETURN result;
END;
$$;
