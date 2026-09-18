-- =============================================================================
-- overall_pnl: daily brand-level P&L written by workers/pnl_worker.py.
--
-- Run this PER BRAND DATABASE before the P&L worker is deployed. The worker
-- itself runs no DDL; it only upserts into this table.
--
-- Idempotent: the CREATE uses IF NOT EXISTS, and the ADD COLUMN for `gst_pct`
-- (added after the table first shipped) is gated on information_schema, so
-- re-running the file is safe.
--
-- Usage, per brand database:
--     mysql -h <host> -u <user> -p<pwd> <brand_db> < 003_overall_pnl.sql
--
-- Column order mirrors MONEY_COLUMNS / INT_COLUMNS / RATE_COLUMNS /
-- TEXT_COLUMNS in workers/pnl_worker.py.
-- =============================================================================

CREATE TABLE IF NOT EXISTS `overall_pnl` (
    `brand_key`               VARCHAR(64)   NOT NULL,
    `date`                    DATE          NOT NULL,
    `gross_sales`             DECIMAL(14,2) NOT NULL DEFAULT 0,
    `discounts`               DECIMAL(14,2) NOT NULL DEFAULT 0,
    `cancellations`           DECIMAL(14,2) NOT NULL DEFAULT 0,
    `gst`                     DECIMAL(14,2) NOT NULL DEFAULT 0,
    `net_sales`               DECIMAL(14,2) NOT NULL DEFAULT 0,
    `cogs`                    DECIMAL(14,2) NOT NULL DEFAULT 0,
    `freight_inwards`         DECIMAL(14,2) NOT NULL DEFAULT 0,
    `gross_margin`            DECIMAL(14,2) NOT NULL DEFAULT 0,
    `shipping`                DECIMAL(14,2) NOT NULL DEFAULT 0,
    `rto`                     DECIMAL(14,2) NOT NULL DEFAULT 0,
    `payment_gateway`         DECIMAL(14,2) NOT NULL DEFAULT 0,
    `packaging`               DECIMAL(14,2) NOT NULL DEFAULT 0,
    `cm1`                     DECIMAL(14,2) NOT NULL DEFAULT 0,
    `meta`                    DECIMAL(14,2) NOT NULL DEFAULT 0,
    `google`                  DECIMAL(14,2) NOT NULL DEFAULT 0,
    `other_paid`              DECIMAL(14,2) NOT NULL DEFAULT 0,
    `cm2`                     DECIMAL(14,2) NOT NULL DEFAULT 0,
    `influencers`             DECIMAL(14,2) NOT NULL DEFAULT 0,
    `content`                 DECIMAL(14,2) NOT NULL DEFAULT 0,
    `sponsorships`            DECIMAL(14,2) NOT NULL DEFAULT 0,
    `other_brand`             DECIMAL(14,2) NOT NULL DEFAULT 0,
    `cm3`                     DECIMAL(14,2) NOT NULL DEFAULT 0,
    `salaries`                DECIMAL(14,2) NOT NULL DEFAULT 0,
    `rent`                    DECIMAL(14,2) NOT NULL DEFAULT 0,
    `technology`              DECIMAL(14,2) NOT NULL DEFAULT 0,
    `agency_fees`             DECIMAL(14,2) NOT NULL DEFAULT 0,
    `other_overheads`         DECIMAL(14,2) NOT NULL DEFAULT 0,
    `ebitda`                  DECIMAL(14,2) NOT NULL DEFAULT 0,
    `rto_returns`             DECIMAL(14,2) NOT NULL DEFAULT 0,
    `cod_sales`               DECIMAL(14,2) NOT NULL DEFAULT 0,
    `prepaid_sales`           DECIMAL(14,2) NOT NULL DEFAULT 0,
    `partially_prepaid_sales` DECIMAL(14,2) NOT NULL DEFAULT 0,
    `total_orders`            INT           NOT NULL DEFAULT 0,
    `rto_orders`              INT           NOT NULL DEFAULT 0,
    `gst_pct`                 DECIMAL(5,2)  NOT NULL DEFAULT 18.00,
    `meta_spend_synced`       TINYINT(1)    NOT NULL DEFAULT 0,
    `cost_config_source`      VARCHAR(16)   NOT NULL DEFAULT 'mongo',
    `computed_at`             TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP
                                            ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`brand_key`, `date`),
    KEY `idx_overall_pnl_date` (`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Columns added after the table first shipped. Guarded so this is a no-op on
-- a fresh table (created above) and on an already-migrated one.
DROP PROCEDURE IF EXISTS _overall_pnl_add_column_if_missing;

DELIMITER $$

CREATE PROCEDURE _overall_pnl_add_column_if_missing(
    IN col VARCHAR(64), IN definition VARCHAR(500)
)
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME   = 'overall_pnl'
          AND COLUMN_NAME  = col
    ) THEN
        SET @s = CONCAT('ALTER TABLE `overall_pnl` ADD COLUMN `', col, '` ', definition);
        PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;
        SELECT CONCAT('  + added overall_pnl.', col) AS log;
    END IF;
END$$

DELIMITER ;

CALL _overall_pnl_add_column_if_missing(
    'gst_pct', 'DECIMAL(5,2) NOT NULL DEFAULT 18.00 AFTER `partially_prepaid_sales`'
);

DROP PROCEDURE IF EXISTS _overall_pnl_add_column_if_missing;

SELECT '=== overall_pnl MIGRATION COMPLETE ===' AS section;
