-- Service roles and grants for the Kalshi ingestor toolkit.
-- Run as a database owner/admin. Roles are created without LOGIN; attach them
-- to service users as needed.

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'kalshi_owner') THEN
    CREATE ROLE kalshi_owner NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'kalshi_rest') THEN
    CREATE ROLE kalshi_rest NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'kalshi_ws') THEN
    CREATE ROLE kalshi_ws NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'kalshi_worker') THEN
    CREATE ROLE kalshi_worker NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'kalshi_rag') THEN
    CREATE ROLE kalshi_rag NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'kalshi_portal') THEN
    CREATE ROLE kalshi_portal NOLOGIN;
  END IF;
END $$;

GRANT USAGE ON SCHEMA public TO
  kalshi_rest, kalshi_ws, kalshi_worker, kalshi_rag, kalshi_portal;

GRANT SELECT ON schema_version TO
  kalshi_rest, kalshi_ws, kalshi_worker, kalshi_rag, kalshi_portal;

-- REST
GRANT SELECT, INSERT, UPDATE, DELETE ON
  events, markets, active_markets, market_candles, ingest_state
TO kalshi_rest;
GRANT SELECT, INSERT ON work_queue TO kalshi_rest;
GRANT SELECT, INSERT, UPDATE ON market_candles_latest TO kalshi_rest;
GRANT SELECT ON market_ticks_latest, market_predictions TO kalshi_rest;
GRANT INSERT, UPDATE, DELETE ON portal_event_rollup TO kalshi_rest;

-- WS
GRANT SELECT, DELETE ON active_markets TO kalshi_ws;
GRANT SELECT ON events TO kalshi_ws;
GRANT SELECT, INSERT, UPDATE ON markets TO kalshi_ws;
GRANT SELECT, INSERT ON market_ticks, lifecycle_events TO kalshi_ws;
GRANT SELECT, INSERT, UPDATE ON market_ticks_latest TO kalshi_ws;
GRANT INSERT, UPDATE, DELETE ON portal_event_rollup TO kalshi_ws;
GRANT SELECT ON market_predictions TO kalshi_ws;
GRANT SELECT, INSERT, UPDATE ON ingest_state TO kalshi_ws;

-- Worker
GRANT SELECT, INSERT ON market_candles TO kalshi_worker;
GRANT SELECT, INSERT, UPDATE ON market_candles_latest TO kalshi_worker;
GRANT SELECT, INSERT, UPDATE ON ingest_state TO kalshi_worker;
GRANT SELECT, UPDATE, DELETE ON work_queue TO kalshi_worker;

-- RAG
GRANT SELECT ON events, markets, market_ticks, market_ticks_latest, market_candles,
  market_candles_latest TO kalshi_rag;
GRANT SELECT, INSERT, UPDATE ON rag_documents TO kalshi_rag;
GRANT SELECT, INSERT, UPDATE ON prediction_runs TO kalshi_rag;
GRANT SELECT, INSERT, UPDATE ON market_predictions TO kalshi_rag;
GRANT SELECT, INSERT, UPDATE ON ingest_state TO kalshi_rag;
GRANT INSERT, UPDATE, DELETE ON portal_event_rollup TO kalshi_rag;

-- Portal (read-only, insert on work_queue when queue-mode backfill is enabled).
GRANT SELECT ON
  events, markets, active_markets, market_ticks, market_ticks_latest,
  lifecycle_events, market_candles, market_candles_latest, ingest_state, work_queue,
  prediction_runs, market_predictions, rag_documents, portal_event_rollup
TO kalshi_portal;
GRANT INSERT ON work_queue TO kalshi_portal;

-- Sequences (needed for inserts into serial/bigserial tables).
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO
  kalshi_rest, kalshi_ws, kalshi_worker, kalshi_rag;
GRANT USAGE, SELECT ON SEQUENCE work_queue_id_seq TO kalshi_portal;

-- Default privileges for future tables/sequences created by the current role.
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO
  kalshi_rest, kalshi_ws, kalshi_worker, kalshi_rag;

ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO
  kalshi_portal;

ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE
ON TABLES TO kalshi_rest;

ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT ON TABLES TO
  kalshi_ws, kalshi_worker;

ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE ON TABLES TO
  kalshi_rag;

-- Ensure existing partitions inherit grants for market_ticks and market_candles.
DO $$
DECLARE r RECORD;
BEGIN
  FOR r IN
    SELECT tablename
    FROM pg_tables
    WHERE schemaname = 'public'
      AND tablename LIKE 'market_ticks_%'
  LOOP
    EXECUTE format('GRANT SELECT, INSERT ON TABLE %I TO kalshi_ws', r.tablename);
    EXECUTE format('GRANT SELECT ON TABLE %I TO kalshi_rag', r.tablename);
    EXECUTE format('GRANT SELECT ON TABLE %I TO kalshi_portal', r.tablename);
  END LOOP;

  FOR r IN
    SELECT tablename
    FROM pg_tables
    WHERE schemaname = 'public'
      AND tablename LIKE 'market_candles_%'
  LOOP
    EXECUTE format('GRANT SELECT, INSERT ON TABLE %I TO kalshi_rest', r.tablename);
    EXECUTE format('GRANT SELECT, INSERT ON TABLE %I TO kalshi_worker', r.tablename);
    EXECUTE format('GRANT SELECT ON TABLE %I TO kalshi_rag', r.tablename);
    EXECUTE format('GRANT SELECT ON TABLE %I TO kalshi_portal', r.tablename);
  END LOOP;
END $$;
