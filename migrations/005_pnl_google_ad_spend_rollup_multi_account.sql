-- =============================================================================
-- Widens pnl_google_ad_spend_rollup.customer_id so it can hold a
-- comma-joined list of ids (e.g. "1234567890,1112223333"), not just one.
--
-- The dashboard's "Connect with Google" can now authorize more than one
-- Google Ads account per brand (arch-auth Mongo: google_ads_credentials.
-- customer_ids, a checkbox picker). workers/pnl_worker.py sums that brand's
-- spend across every authorized account into the SAME one-row-per-date
-- shape this table already had (see 004_pnl_ad_spend_rollups.sql) - no
-- change to the unique key or to how the dashboard reads this table
-- (pnl.service.js / pnlAdSpendRollup.service.js only ever SUM(spend) and
-- COUNT(*) per date range, never touch customer_id) - just a wider column
-- to record which accounts contributed.
--
-- Run this PER BRAND DATABASE, after 004_pnl_ad_spend_rollups.sql.
--
-- Idempotent, and guarded two ways so the automatic runner
-- (pipeline/sql_migrations.py) can apply it unattended:
--
--   * `pnl_google_ad_spend_rollup` may not exist yet in a brand database that
--     has never run the P&L worker. 004 creates it with CREATE TABLE IF NOT
--     EXISTS and runs first, but the guard below keeps this file from failing
--     with errno 1146 if it is ever run on its own.
--   * MODIFY COLUMN has no IF NOT EXISTS form and is not free: it is a
--     declarative rewrite, so re-running it re-validates every row. The
--     information_schema check makes the second and every later run a no-op
--     rather than a repeated table rebuild.
--
-- Usage, per brand database:
--     mysql -h <host> -u <user> -p<pwd> <brand_db> < 005_pnl_google_ad_spend_rollup_multi_account.sql
-- =============================================================================

DROP PROCEDURE IF EXISTS _widen_google_rollup_customer_id;

DELIMITER $$

CREATE PROCEDURE _widen_google_rollup_customer_id()
BEGIN
    DECLARE current_len INT DEFAULT NULL;

    SELECT CHARACTER_MAXIMUM_LENGTH INTO current_len
    FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME   = 'pnl_google_ad_spend_rollup'
      AND COLUMN_NAME  = 'customer_id';

    IF current_len IS NULL THEN
        SELECT '  = pnl_google_ad_spend_rollup.customer_id absent (table or column not created yet), skipping' AS log;
    ELSEIF current_len >= 512 THEN
        SELECT '  = pnl_google_ad_spend_rollup.customer_id already >= VARCHAR(512)' AS log;
    ELSE
        ALTER TABLE `pnl_google_ad_spend_rollup`
            MODIFY COLUMN `customer_id` VARCHAR(512) NULL
                COMMENT 'digits-only customer id(s) the spend was pulled for, comma-joined when more than one account was authorized';
        SELECT '  + widened pnl_google_ad_spend_rollup.customer_id to VARCHAR(512)' AS log;
    END IF;
END$$

DELIMITER ;

CALL _widen_google_rollup_customer_id();

DROP PROCEDURE IF EXISTS _widen_google_rollup_customer_id;

SELECT '=== 005_pnl_google_ad_spend_rollup_multi_account MIGRATION COMPLETE ===' AS section;
