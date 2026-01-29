-- Patch: make portal rollup refreshes non-blocking per event to avoid deadlocks.
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
    SUM(tv.volume) AS volume,
    COALESCE(BOOL_OR(mt.has_tick), false) AS has_tick,
    COALESCE(BOOL_OR(LOWER(am.status) IN ('open', 'active')), false) AS has_open_status,
    COALESCE(BOOL_OR(LOWER(am.status) = 'paused'), false) AS has_paused_status,
    p.predicted_yes_prob AS agent_yes_prob,
    p.confidence AS agent_confidence,
    p.created_at AS agent_prediction_ts,
    p.market_ticker AS agent_market_ticker
  INTO v_row
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
    agent_yes_prob = EXCLUDED.agent_yes_prob,
    agent_confidence = EXCLUDED.agent_confidence,
    agent_prediction_ts = EXCLUDED.agent_prediction_ts,
    agent_market_ticker = EXCLUDED.agent_market_ticker,
    updated_at = NOW();
END $$;
