-- =============================================================================
-- Campaign / ad / product-level paid-ads performance, written by
-- workers/ads_performance.py (called from workers/pnl_worker.py right after
-- the account-level spend rollups in 004_pnl_ad_spend_rollups.sql) and read
-- by the dashboard's Ads page (analytics/services/adsPerformance.service.js).
--
-- Same credentials and the same target dates as the P&L spend sync: a brand
-- that connected Meta / Google on the P&L page gets these too, and the
-- dashboard's "connect -> backfill 30 days" trigger fills them as well.
--
-- All three tables hold one row per (channel, date, <entity>). The worker
-- replaces a channel's rows for exactly the dates it fetched (DELETE that
-- window, then INSERT), so a campaign that stopped spending doesn't leave a
-- stale row behind. A day with no rows was either never synced or had no
-- delivery.
--
--   channel             'meta' | 'google'
--   platform_purchases  / platform_purchase_value: what the ad platform
--                       itself reports (Meta omni_purchase action /
--                       Google conversions + conversions_value)
--   utm_orders          / utm_sales: Shopify orders from
--                       shopify_orders_utm_daily whose UTMs carry this
--                       campaign's (or ad's) id or name - the dashboard's own
--                       attribution, independent of the platform's
--   link_clicks         / landing_page_views: Meta only (inline_link_clicks /
--                       landing_page_view action), NULL for Google
--
-- Run this PER BRAND DATABASE. The worker runs no DDL; it skips the ads sync
-- (with a warning) when these tables are missing.
--
-- Idempotent: every CREATE uses IF NOT EXISTS.
--
-- Usage, per brand database:
--     mysql -h <host> -u <user> -p<pwd> <brand_db> < 007_ads_performance.sql
-- =============================================================================

CREATE TABLE IF NOT EXISTS `ads_campaign_daily` (
    `id`                      BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `date`                    DATE            NOT NULL,
    `channel`                 VARCHAR(16)     NOT NULL,
    `account_id`              VARCHAR(64)     NULL,
    `campaign_id`             VARCHAR(64)     NOT NULL,
    `campaign_name`           VARCHAR(512)    NULL,
    `campaign_status`         VARCHAR(32)     NULL,  -- Meta effective_status / Google campaign.status
    `campaign_type`           VARCHAR(64)     NULL,  -- Meta objective / Google advertising_channel_type
    `currency`                VARCHAR(8)      NULL,
    `spend`                   DECIMAL(14,4)   NOT NULL DEFAULT 0,
    `impressions`             BIGINT          NOT NULL DEFAULT 0,
    `clicks`                  BIGINT          NOT NULL DEFAULT 0,
    `link_clicks`             BIGINT          NULL,
    `landing_page_views`      BIGINT          NULL,
    `platform_purchases`      DECIMAL(14,4)   NOT NULL DEFAULT 0,
    `platform_purchase_value` DECIMAL(14,4)   NOT NULL DEFAULT 0,
    `utm_orders`              INT             NOT NULL DEFAULT 0,
    `utm_sales`               DECIMAL(14,2)   NOT NULL DEFAULT 0,
    `synced_at`               TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP
                                              ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uniq_ads_campaign_day` (`channel`, `date`, `campaign_id`),
    KEY `idx_ads_campaign_date` (`date`, `channel`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Meta: one row per ad. Google: one row per ad group (ad_id = ad_group.id,
-- adset_* left NULL) - Google's per-ad rows add volume without adding
-- anything the Ads page shows.
CREATE TABLE IF NOT EXISTS `ads_ad_daily` (
    `id`                      BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `date`                    DATE            NOT NULL,
    `channel`                 VARCHAR(16)     NOT NULL,
    `account_id`              VARCHAR(64)     NULL,
    `campaign_id`             VARCHAR(64)     NOT NULL,
    `adset_id`                VARCHAR(64)     NULL,
    `adset_name`              VARCHAR(512)    NULL,
    `ad_id`                   VARCHAR(64)     NOT NULL,
    `ad_name`                 VARCHAR(512)    NULL,
    `landing_url`             VARCHAR(1024)   NULL,  -- creative destination URL (Meta)
    `landing_path`            VARCHAR(512)    NULL,  -- normalized path of landing_url, joins web_vitals_summary
    `product_id`              BIGINT          NULL,  -- from landing_path's /products/<handle>, NULL if not a product page
    `spend`                   DECIMAL(14,4)   NOT NULL DEFAULT 0,
    `impressions`             BIGINT          NOT NULL DEFAULT 0,
    `clicks`                  BIGINT          NOT NULL DEFAULT 0,
    `link_clicks`             BIGINT          NULL,
    `landing_page_views`      BIGINT          NULL,
    `platform_purchases`      DECIMAL(14,4)   NOT NULL DEFAULT 0,
    `platform_purchase_value` DECIMAL(14,4)   NOT NULL DEFAULT 0,
    `utm_orders`              INT             NOT NULL DEFAULT 0,
    `utm_sales`               DECIMAL(14,2)   NOT NULL DEFAULT 0,
    `synced_at`               TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP
                                              ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uniq_ads_ad_day` (`channel`, `date`, `ad_id`),
    KEY `idx_ads_ad_date_campaign` (`date`, `channel`, `campaign_id`),
    KEY `idx_ads_ad_date_path` (`date`, `landing_path`(191))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Spend per product per channel per day. product_id 0 = spend that couldn't
-- be tied to one product (catalog/collection/home-page ads, Search campaigns,
-- Shopping items whose id doesn't carry a Shopify product id), so a
-- channel's product rows always sum to its campaign spend for the day.
--   source 'creative' -> Meta ad destination URL /products/<handle>
--   source 'shopping' -> Google shopping_performance_view product_item_id
--   source 'unattributed' -> the product_id 0 remainder
CREATE TABLE IF NOT EXISTS `ads_product_daily` (
    `id`                      BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `date`                    DATE            NOT NULL,
    `channel`                 VARCHAR(16)     NOT NULL,
    `product_id`              BIGINT          NOT NULL DEFAULT 0,
    `source`                  VARCHAR(16)     NOT NULL,
    `spend`                   DECIMAL(14,4)   NOT NULL DEFAULT 0,
    `impressions`             BIGINT          NOT NULL DEFAULT 0,
    `clicks`                  BIGINT          NOT NULL DEFAULT 0,
    `platform_purchase_value` DECIMAL(14,4)   NOT NULL DEFAULT 0,
    `synced_at`               TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP
                                              ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uniq_ads_product_day` (`channel`, `date`, `product_id`),
    KEY `idx_ads_product_date` (`date`, `product_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

SELECT '=== 007_ads_performance MIGRATION COMPLETE ===' AS section;
