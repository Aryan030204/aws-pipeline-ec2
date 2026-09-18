-- =============================================================================
-- Daily paid-ads spend per channel, written by workers/pnl_worker.py right
-- before it builds overall_pnl, for brands that pasted their Meta / Google
-- Ads credentials on the dashboard's P&L page (arch-auth Mongo:
-- meta_ads_credentials / google_ads_credentials, one document per brand).
--
-- One row per calendar date per table. The worker pulls exactly the dates it
-- is rebuilding (its rolling window, or an explicit backfill range) and
-- upserts them; a fetched day with no spend is written as 0, a day the sync
-- never covered simply has no row. The dashboard's P&L reads these tables
-- directly for the Meta and Google lines and shows "-" for a range with no
-- rows, so "no row" and "0" mean different things and must stay that way.
--
-- Run this PER BRAND DATABASE, after 003_overall_pnl.sql. The worker itself
-- runs no DDL; it skips a channel (with a warning) when its table is missing.
--
-- Idempotent: both CREATEs use IF NOT EXISTS.
--
-- Usage, per brand database:
--     mysql -h <host> -u <user> -p<pwd> <brand_db> < 004_pnl_ad_spend_rollups.sql
-- =============================================================================

-- Meta (Facebook) Marketing API, account-level insights, `spend` field.
CREATE TABLE IF NOT EXISTS `pnl_meta_ad_spend_rollup` (
    `id`            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `date`          DATE            NOT NULL,
    `ad_account_id` VARCHAR(64)     NULL,             -- act_<id> the spend was pulled for
    `spend`         DECIMAL(14,4)   NOT NULL DEFAULT 0,
    `currency`      VARCHAR(8)      NULL,             -- account_currency as reported by Meta
    `synced_at`     TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP
                                    ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uniq_pnl_meta_ad_spend_date` (`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Google Ads API, googleAds:search over `customer` segmented by date,
-- metrics.cost_micros / 1e6.
CREATE TABLE IF NOT EXISTS `pnl_google_ad_spend_rollup` (
    `id`            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `date`          DATE            NOT NULL,
    `customer_id`   VARCHAR(32)     NULL,             -- digits-only customer id the spend was pulled for
    `spend`         DECIMAL(14,4)   NOT NULL DEFAULT 0,
    `currency`      VARCHAR(8)      NULL,             -- customer.currency_code as reported by Google
    `synced_at`     TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP
                                    ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uniq_pnl_google_ad_spend_date` (`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

SELECT '=== pnl_ad_spend_rollups MIGRATION COMPLETE ===' AS section;
