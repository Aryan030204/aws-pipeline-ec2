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
-- Idempotent: MODIFY COLUMN is safe to re-run.
--
-- Usage, per brand database:
--     mysql -h <host> -u <user> -p<pwd> <brand_db> < 005_pnl_google_ad_spend_rollup_multi_account.sql
-- =============================================================================

ALTER TABLE `pnl_google_ad_spend_rollup`
    MODIFY COLUMN `customer_id` VARCHAR(512) NULL
        COMMENT 'digits-only customer id(s) the spend was pulled for, comma-joined when more than one account was authorized';

SELECT '=== 005_pnl_google_ad_spend_rollup_multi_account MIGRATION COMPLETE ===' AS section;
