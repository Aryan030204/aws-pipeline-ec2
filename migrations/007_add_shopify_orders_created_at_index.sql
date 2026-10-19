-- =============================================================================
-- Add the index that pipeline/orchestration.py's get_last_order() actually
-- needs on shopify_orders.
--
-- _get_last_order_with_cursor() (pipeline/orchestration.py and the identical
-- copy in `backfill scripts/aws_data_update.py`) runs:
--     SELECT order_id, created_at, updated_at FROM shopify_orders
--     ORDER BY created_at DESC LIMIT 1
-- for the NEW-orders watermark, every incremental pipeline pass, every brand.
--
-- migrations/002_converge_to_canonical.sql's canonical index set for
-- shopify_orders has `idx_updated_at` (updated_at) and `idx_customer_created`
-- (customer_id, created_at) - created_at only ever appears as the second
-- column of a composite, never as the leading column of its own index. So
-- `ORDER BY created_at DESC LIMIT 1` has no usable index: it's a full table
-- scan + filesort over the whole table to fetch one row.
--
-- Confirmed in production: this exact query timed out (mysql.connector error
-- 3024, "The Read Operation timed out") against DB_READ_TIMEOUT_S on brand
-- AJMAL during a backfill - shopify_orders_update's equivalent query (which
-- orders by updated_at and IS covered by idx_updated_at) was not affected.
--
-- This migration is purely additive and idempotent - safe to run more than
-- once. Run per brand database (same convention as 001/002/006):
--     mysql -h <host> -u <user> -p<pwd> <brand_db> < 007_add_shopify_orders_created_at_index.sql
--
-- Verify first:
--     EXPLAIN SELECT order_id, created_at, updated_at FROM shopify_orders
--     ORDER BY created_at DESC LIMIT 1;
--   -> if `key` is NULL and `type` is ALL, this migration is needed.
-- Verify after: re-run the same EXPLAIN - `key` should now show
-- idx_so_created_at, and `rows` should drop to 1.
-- =============================================================================

DROP PROCEDURE IF EXISTS _create_index_if_missing_007;

DELIMITER $$

CREATE PROCEDURE _create_index_if_missing_007(
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

-- ── shopify_orders(created_at) ──────────────────────────────────────────────
-- Single-column, leading on created_at, so `ORDER BY created_at DESC LIMIT 1`
-- can walk the index backwards for one row instead of scanning the table.
CALL _create_index_if_missing_007('shopify_orders', 'idx_so_created_at',
    '`created_at`');

DROP PROCEDURE IF EXISTS _create_index_if_missing_007;

SELECT '=== 007_add_shopify_orders_created_at_index COMPLETE ===' AS section;
