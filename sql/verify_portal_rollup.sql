-- Sanity checks for portal_event_rollup vs live aggregation.
-- Replace the event tickers in target_events before running.

WITH target_events AS (
  SELECT unnest(ARRAY['EV1', 'EV2']) AS event_ticker
),
live AS (
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
  WHERE e.event_ticker IN (SELECT event_ticker FROM target_events)
  GROUP BY
    e.event_ticker,
    e.title,
    e.sub_title,
    e.category,
    e.strike_period,
    p.predicted_yes_prob,
    p.confidence,
    p.created_at,
    p.market_ticker
)
SELECT
  r.event_ticker,
  (r.event_title = l.event_title) AS event_title_ok,
  (r.event_sub_title IS NOT DISTINCT FROM l.event_sub_title) AS event_sub_title_ok,
  (r.event_category IS NOT DISTINCT FROM l.event_category) AS event_category_ok,
  (r.strike_period IS NOT DISTINCT FROM l.strike_period) AS strike_period_ok,
  (r.open_time IS NOT DISTINCT FROM l.open_time) AS open_time_ok,
  (r.close_time IS NOT DISTINCT FROM l.close_time) AS close_time_ok,
  (r.market_count = l.market_count) AS market_count_ok,
  (r.closed_market_count = l.closed_market_count) AS closed_market_count_ok,
  (r.volume IS NOT DISTINCT FROM l.volume) AS volume_ok,
  (r.has_tick = l.has_tick) AS has_tick_ok,
  (r.has_open_status = l.has_open_status) AS has_open_status_ok,
  (r.has_paused_status = l.has_paused_status) AS has_paused_status_ok,
  (r.agent_yes_prob IS NOT DISTINCT FROM l.agent_yes_prob) AS agent_yes_prob_ok,
  (r.agent_confidence IS NOT DISTINCT FROM l.agent_confidence) AS agent_confidence_ok,
  (r.agent_prediction_ts IS NOT DISTINCT FROM l.agent_prediction_ts) AS agent_prediction_ts_ok,
  (r.agent_market_ticker IS NOT DISTINCT FROM l.agent_market_ticker) AS agent_market_ticker_ok
FROM portal_event_rollup r
JOIN live l USING (event_ticker)
ORDER BY r.event_ticker;

-- Count events missing from rollup (should be 0).
SELECT COUNT(*) AS missing_rollups
FROM (
  SELECT e.event_ticker
  FROM events e
  JOIN markets m ON m.event_ticker = e.event_ticker
  GROUP BY e.event_ticker
) live_events
LEFT JOIN portal_event_rollup r USING (event_ticker)
WHERE r.event_ticker IS NULL;
