-- =============================================================================
-- Restore indexes matching the pipeline's ACTUAL query pattern.
--
-- migrations/001_rds_perf_pass.sql and 002_converge_to_canonical.sql both
-- assume the pipeline was patched to query shopify_orders/shopify_orders_update
-- via the generated `created_dt`/`created_hr`/`updated_dt` columns, and drop
-- every index built on the plain VARCHAR `created_date`/`updated_date`/
-- `created_time` columns on that basis.
--
-- That patch does not exist in the current pipeline/ package: as of this
-- writing, pipeline/summary_incremental.py still queries shopify_orders and
-- shopify_orders_update via `WHERE created_date IN (...)`,
-- `STR_TO_DATE(created_date, '%Y-%m-%d')`, and `HOUR(created_time)` at
-- ~15 call sites (sales/order/discount/gross/hour-wise-sales summaries,
-- daily+hourly citywise summary, all 4 dashboard-discount tables,
-- overall_summary, rto_summary, shopify_orders UTM daily) - every one of
-- which runs on every incremental pipeline pass, for every brand. If 001's
-- Phase 2/2b index drops were ever applied to a brand's database, these are
-- full table scans today.
--
-- This migration is purely additive (new indexes only, nothing dropped) and
-- fully idempotent - safe to run more than once, and safe to run whether or
-- not the old varchar indexes from before 001 still exist on this brand.
-- Run per brand database (same convention as 001/002):
--     mysql -h <host> -u <user> -p<pwd> <brand_db> < 006_restore_order_date_indexes.sql
--
-- Verify first (before running, on one representative brand):
--     EXPLAIN SELECT * FROM shopify_orders WHERE created_date IN ('2026-09-01');
--   -> if `key` is NULL and `type` is ALL, this migration is needed.
-- Verify after:
--     re-run the same EXPLAIN - `key` should now show idx_so_created_date
--     (or idx_so_created_date_time for a query that also touches created_time),
--     and `rows` should drop from ~the whole table to roughly one day's orders.
-- =============================================================================

DROP PROCEDURE IF EXISTS _create_index_if_missing_006;

DELIMITER $$

-- Same idempotent-create pattern as migration 002's _create_index_if_missing,
-- kept local to this file (own name) so this migration has no dependency on
-- 002 having been run first or its procedures still existing.
CREATE PROCEDURE _create_index_if_missing_006(
    IN tbl VARCHAR(64), IN idx VARCHAR(64), IN cols VARCHAR(500)
)
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.STATISTICS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME   = tbl
          AND INDEX_NAME   = idx
    ) THEN
        SET @s = CONCAT('ALTER TABLE `', tbl, '` ADD INDEX `', idx, '` (',
                        cols, '), ALGORITHM=INPLACE, LOCK=NONE');
        PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;
        SELECT CONCAT('  + created ', tbl, '.', idx) AS log;
    ELSE
        SELECT CONCAT('  = ', tbl, '.', idx, ' already present') AS log;
    END IF;
END$$

DELIMITER ;

-- ── shopify_orders(created_date) ────────────────────────────────────────────
-- The single most-hit predicate: `WHERE created_date IN (...)` in
-- update_sales_summary_incremental, update_order_summary_incremental,
-- update_discount_summary_incremental, update_gross_summary_incremental,
-- update_daily_citywise_summary_incremental, all 4 dashboard-discount daily/
-- hourly incrementals, and update_shopify_orders_utm_daily_incremental.
CALL _create_index_if_missing_006('shopify_orders', 'idx_so_created_date',
    '`created_date`');

-- ── shopify_orders(created_date, created_time) ──────────────────────────────
-- Covers the HOUR(created_time) grouping in update_hour_wise_sales_incremental
-- and the payment-mode-breakdown hourly queries without a second lookup once
-- the created_date row set is narrowed.
CALL _create_index_if_missing_006('shopify_orders', 'idx_so_created_date_time',
    '`created_date`, `created_time`');

-- ── shopify_orders_update(updated_date) ─────────────────────────────────────
-- Mirrors the NEW-orders index above for the UPDATED-orders path.
CALL _create_index_if_missing_006('shopify_orders_update', 'idx_sou_updated_date',
    '`updated_date`');

DROP PROCEDURE IF EXISTS _create_index_if_missing_006;

SELECT '=== 006_restore_order_date_indexes COMPLETE ===' AS section;
