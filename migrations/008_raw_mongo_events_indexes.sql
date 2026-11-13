-- 008: leaner indexes on raw_mongo_events (run per brand database, off-peak, one brand at a time).
--
-- Why: the CI / buy-now rollups read "this event type in this day window" every 15 minutes. The old
-- idx_created_at_utc holds only (created_at_utc, event_id), so every row of the day needed a clustered
-- lookup of a wide row and event_type was filtered afterwards. (event_type, created_at_utc) lets MySQL
-- read only the checkout_initiated / buy_now rows. Fewer secondary indexes also means fewer writes per
-- ingested event. Tables created after this change already have the new layout; only run this on
-- existing tables (their old layout has idx_event_type, idx_session_id, idx_cart_token).
--
-- Replace <BRAND_DB> with each brand database (PTS, BBB, TMC, AJMAL, VAMA, 3TENX, SHYLENEW, ...).

-- 1. Add the composite index. ONLINE: reads and writes continue. One-time index build = a burst of I/O
--    proportional to table size, so do this in a quiet window (and watch EBS balance), one brand at a time.
--    Run it from the mysql CLI (Workbench's 60 s read timeout will cut a long statement off).
ALTER TABLE <BRAND_DB>.raw_mongo_events
  ADD INDEX idx_type_created (event_type, created_at_utc),
  ALGORITHM=INPLACE, LOCK=NONE;

-- 2. idx_event_type is now redundant (idx_type_created starts with event_type). Dropping is cheap.
ALTER TABLE <BRAND_DB>.raw_mongo_events
  DROP INDEX idx_event_type,
  ALGORITHM=INPLACE, LOCK=NONE;

-- 3. OPTIONAL, only after confirming nothing else (other services on the host, dashboards) queries by
--    session_id or cart_token. Check first (needs performance_schema; counts reset on restart):
--      SELECT * FROM sys.schema_unused_indexes WHERE object_schema='<BRAND_DB>' AND object_name='raw_mongo_events';
--    This repo never queries raw_mongo_events by session_id / cart_token alone.
-- ALTER TABLE <BRAND_DB>.raw_mongo_events
--   DROP INDEX idx_session_id,
--   DROP INDEX idx_cart_token,
--   ALGORITHM=INPLACE, LOCK=NONE;

-- Verify (expect type=range, key=idx_type_created, far fewer rows than the day's total):
-- EXPLAIN SELECT event_type, session_id, cart_token, variantId, created_at_utc
--   FROM <BRAND_DB>.raw_mongo_events
--  WHERE created_at_utc >= '2026-11-12 00:00:00' AND created_at_utc < '2026-11-13 00:00:00'
--    AND event_type IN ('checkout_initiated','buy_now');
