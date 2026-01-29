# Portal Rollup Verification

## SQL sanity checks
1) Edit `kalshi_ingestor/sql/verify_portal_rollup.sql` and replace `EV1`, `EV2`
   with real event tickers.
2) Run:
   ```
   psql "$DATABASE_URL" -f kalshi_ingestor/sql/verify_portal_rollup.sql
   ```
3) Confirm all `*_ok` columns are `t` and `missing_rollups` is `0`.

## Portal route checks
Use any event/market ticker that appears on the portal.
1) Load the portal index:
   ```
   curl -s "http://localhost:8123/" | head -n 20
   ```
2) Load an event page:
   ```
   curl -s "http://localhost:8123/event/<EVENT_TICKER>" | head -n 20
   ```
3) Load a market page:
   ```
   curl -s "http://localhost:8123/market/<MARKET_TICKER>" | head -n 20
   ```

Compare the totals/status labels/time windows shown in the UI against:
```
SELECT portal_snapshot_json(
  200, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 900,
  NULL, NULL,
  NULL, NULL,
  NULL, NULL,
  TRUE, TRUE
);
```
