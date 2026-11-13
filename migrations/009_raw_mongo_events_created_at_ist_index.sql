-- 009: index raw_mongo_events.created_at_ist so the monthly data purge can use it (run per brand database,
--      off-peak, one brand at a time, from the mysql CLI - Workbench's 60 s read timeout will cut it off).
--
-- Why: workers/data_purge_worker.py purges raw_mongo_events by created_at_ist (IST wall-clock DATETIME(3)).
-- The purge validates the live schema first and SKIPS any table whose purge date column does not lead an
-- index (SKIPPED_NO_INDEX), so raw_mongo_events is not archived or deleted until this index exists.
-- Existing indexes (PK event_id, idx_type_created, idx_created_at_utc) do not lead with created_at_ist.
--
-- Cost: ONLINE (reads and writes continue) but the one-time build reads the whole wide table and writes
-- a new index = a burst of I/O proportional to table size (watch EBS balance), and every ingested event
-- then maintains one more secondary index. Tables created by the pipeline do NOT get this index
-- automatically (deliberately: it is only needed for the purge). Apply it only where the purge will run.
--
-- Equivalent alternative (no new index): purge by created_at_utc using the cutoff 2026-03-31 18:30:00 UTC
-- (IST midnight of the cutoff date). For IST brands it selects exactly the same rows.
--
-- Replace <BRAND_DB> with each brand database (PTS, BBB, TMC, AJMAL, VAMA, 3TENX, SHYLENEW, ...).

ALTER TABLE <BRAND_DB>.raw_mongo_events
  ADD INDEX idx_created_at_ist (created_at_ist),
  ALGORITHM=INPLACE, LOCK=NONE;

-- Verify (expect type=range, key=idx_created_at_ist, no filesort):
-- EXPLAIN SELECT * FROM <BRAND_DB>.raw_mongo_events
--  WHERE created_at_ist < '2026-04-01 00:00:00' ORDER BY created_at_ist LIMIT 1000;
-- EXPLAIN DELETE FROM <BRAND_DB>.raw_mongo_events
--  WHERE created_at_ist < '2026-04-01 00:00:00' ORDER BY created_at_ist LIMIT 1000;
