-- =============================================================================
-- Converge ANY brand database to the canonical post-cleanup state.
--
-- Fully idempotent: safe to run on a fresh DB, or on one that's already been
-- partially migrated, or twice in a row. Every change is gated on an
-- information_schema lookup.
--
-- What it converges:
--   1. Summary tables get PRIMARY KEY(`date`) (or (date, hour) for hour_wise).
--   2. *_stage tables get the same PK.
--   3. `shopify_orders` ends up with exactly the 11 canonical indexes.
--   4. `shopify_orders_update` ends up with exactly the 4 canonical indexes.
--   5. At the end, prints any leftover indexes NOT in the canonical sets so
--      you can review them case-by-case.
--
-- Usage, per brand database (loop in your shell, Python, or whatever):
--     mysql -h <host> -u <user> -p<pwd> <brand_db> < 002_converge_to_canonical.sql
--
-- The script runs inline ALTERs for index add/drop (ALGORITHM=INPLACE,
-- LOCK=NONE — cheap and online). For PRIMARY KEY adds it uses
-- ALGORITHM=COPY because converting a nullable column to NOT NULL under
-- strict sql_mode rebuilds the table — summary tables are tiny so this is
-- seconds. shopify_orders_update is NOT force-rebuilt here; PK on that
-- table is out of scope (run Phase 3 of 001_rds_perf_pass.sql separately if
-- you want it).
-- =============================================================================

-- We do not modify sql_mode — the script honours whatever strict mode the
-- server has. Pre-flight checks ensure every ALTER will succeed before it
-- runs, so the script never partially applies.


-- ============================================================================
-- HELPER PROCEDURES
-- ============================================================================

DROP PROCEDURE IF EXISTS _drop_index_if_exists;
DROP PROCEDURE IF EXISTS _create_index_if_missing;
DROP PROCEDURE IF EXISTS _add_date_pk_if_missing;
DROP PROCEDURE IF EXISTS _add_date_hour_pk_if_missing;

DELIMITER $$

-- Drop an index if it exists; no-op otherwise.
CREATE PROCEDURE _drop_index_if_exists(IN tbl VARCHAR(64), IN idx VARCHAR(64))
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.STATISTICS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME   = tbl
          AND INDEX_NAME   = idx
    ) THEN
        SET @s = CONCAT('ALTER TABLE `', tbl, '` DROP INDEX `', idx,
                        '`, ALGORITHM=INPLACE, LOCK=NONE');
        PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;
        SELECT CONCAT('  - dropped ', tbl, '.', idx) AS log;
    END IF;
END$$

-- Create an index with the exact canonical name+columns if missing.
-- `cols` is a raw column list, e.g. '`created_dt`, `utm_source`' or
-- '`payment_gateway_names`(50)'.
CREATE PROCEDURE _create_index_if_missing(
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
    END IF;
END$$

-- Add PRIMARY KEY(`date`) to a table if missing, with pre-flight NULL and
-- duplicate checks. Skips (with a warning) if unsafe.
CREATE PROCEDURE _add_date_pk_if_missing(IN tbl VARCHAR(64))
BEGIN
    DECLARE has_pk INT DEFAULT 0;

    SELECT COUNT(*) INTO has_pk FROM information_schema.TABLE_CONSTRAINTS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME   = tbl
      AND CONSTRAINT_TYPE = 'PRIMARY KEY';

    IF has_pk > 0 THEN
        SELECT CONCAT('  = ', tbl, ': PK already present') AS log;
    ELSE
        -- NULL check
        SET @nq = CONCAT('SELECT COUNT(*) INTO @null_c FROM `', tbl,
                         '` WHERE `date` IS NULL');
        PREPARE stmt FROM @nq; EXECUTE stmt; DEALLOCATE PREPARE stmt;

        -- DUP check
        SET @dq = CONCAT('SELECT COUNT(*) INTO @dup_c FROM ',
                         '(SELECT `date` FROM `', tbl,
                         '` GROUP BY `date` HAVING COUNT(*) > 1) t');
        PREPARE stmt FROM @dq; EXECUTE stmt; DEALLOCATE PREPARE stmt;

        IF @null_c > 0 THEN
            SELECT CONCAT('  ! ', tbl, ': ', @null_c,
                          ' rows have NULL date — resolve manually, skipping') AS log;
        ELSEIF @dup_c > 0 THEN
            SELECT CONCAT('  ! ', tbl, ': ', @dup_c,
                          ' duplicate date groups — resolve manually, skipping') AS log;
        ELSE
            SET @a = CONCAT('ALTER TABLE `', tbl, '` ',
                            'MODIFY `date` DATE NOT NULL, ',
                            'ADD PRIMARY KEY (`date`), ',
                            'ALGORITHM=COPY, LOCK=SHARED');
            PREPARE stmt FROM @a; EXECUTE stmt; DEALLOCATE PREPARE stmt;
            SELECT CONCAT('  + added PK(date) on ', tbl) AS log;
        END IF;
    END IF;
END$$

-- Add composite PRIMARY KEY(`date`, `hour`) to hour_wise tables.
CREATE PROCEDURE _add_date_hour_pk_if_missing(IN tbl VARCHAR(64))
BEGIN
    DECLARE has_pk INT DEFAULT 0;

    SELECT COUNT(*) INTO has_pk FROM information_schema.TABLE_CONSTRAINTS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME   = tbl
      AND CONSTRAINT_TYPE = 'PRIMARY KEY';

    IF has_pk > 0 THEN
        SELECT CONCAT('  = ', tbl, ': PK already present') AS log;
    ELSE
        SET @nq = CONCAT('SELECT COUNT(*) INTO @null_c FROM `', tbl,
                         '` WHERE `date` IS NULL OR `hour` IS NULL');
        PREPARE stmt FROM @nq; EXECUTE stmt; DEALLOCATE PREPARE stmt;

        SET @dq = CONCAT('SELECT COUNT(*) INTO @dup_c FROM ',
                         '(SELECT `date`, `hour` FROM `', tbl,
                         '` GROUP BY `date`, `hour` HAVING COUNT(*) > 1) t');
        PREPARE stmt FROM @dq; EXECUTE stmt; DEALLOCATE PREPARE stmt;

        IF @null_c > 0 THEN
            SELECT CONCAT('  ! ', tbl, ': ', @null_c,
                          ' rows have NULL date/hour — resolve manually, skipping') AS log;
        ELSEIF @dup_c > 0 THEN
            SELECT CONCAT('  ! ', tbl, ': ', @dup_c,
                          ' duplicate (date,hour) groups — resolve manually, skipping') AS log;
        ELSE
            SET @a = CONCAT('ALTER TABLE `', tbl, '` ',
                            'MODIFY `date` DATE NOT NULL, ',
                            'MODIFY `hour` INT NOT NULL, ',
                            'ADD PRIMARY KEY (`date`, `hour`), ',
                            'ALGORITHM=COPY, LOCK=SHARED');
            PREPARE stmt FROM @a; EXECUTE stmt; DEALLOCATE PREPARE stmt;
            SELECT CONCAT('  + added PK(date,hour) on ', tbl) AS log;
        END IF;
    END IF;
END$$

DELIMITER ;


-- ============================================================================
-- STEP 1: Summary tables — PRIMARY KEY(`date`)
-- ============================================================================

SELECT '=== STEP 1: summary table PKs ===' AS section;

CALL _add_date_pk_if_missing('overall_summary');
CALL _add_date_pk_if_missing('sales_summary');
CALL _add_date_pk_if_missing('gross_summary');
CALL _add_date_pk_if_missing('discount_summary');
CALL _add_date_pk_if_missing('order_summary');
CALL _add_date_hour_pk_if_missing('hour_wise_sales');


-- ============================================================================
-- STEP 2: Stage tables — same PK so range REPLACE/INSERT from stage is keyed
-- ============================================================================

SELECT '=== STEP 2: stage table PKs ===' AS section;

CALL _add_date_pk_if_missing('overall_summary_stage');
CALL _add_date_pk_if_missing('sales_summary_stage');
CALL _add_date_pk_if_missing('gross_summary_stage');
CALL _add_date_pk_if_missing('discount_summary_stage');
CALL _add_date_pk_if_missing('order_summary_stage');
CALL _add_date_hour_pk_if_missing('hour_wise_sales_stage');


-- ============================================================================
-- STEP 3: shopify_orders — converge to the 11-index canonical set
-- ============================================================================

SELECT '=== STEP 3: shopify_orders indexes ===' AS section;

-- Known obsolete / varchar / duplicate / misnamed indexes: drop if present.
CALL _drop_index_if_exists('shopify_orders', 'idx_created_at_order_id');
CALL _drop_index_if_exists('shopify_orders', 'idx_order_app_name');
CALL _drop_index_if_exists('shopify_orders', 'idx_created_date_totalprice');
CALL _drop_index_if_exists('shopify_orders', 'idx_so_created_date_time');
CALL _drop_index_if_exists('shopify_orders', 'idx_so_created_date_order');
CALL _drop_index_if_exists('shopify_orders', 'idx_so_created_date_utm_source');
CALL _drop_index_if_exists('shopify_orders', 'idx_so_created_date_utm_medium');
CALL _drop_index_if_exists('shopify_orders', 'idx_so_created_date_utm_campaign');
CALL _drop_index_if_exists('shopify_orders', 'idx_so_created_date_order_app');
CALL _drop_index_if_exists('shopify_orders', 'idx_so_created_dt_order_app');
CALL _drop_index_if_exists('shopify_orders', 'idx_product_created_qty');
CALL _drop_index_if_exists('shopify_orders', 'idx_created_product_linte_item_qty');
-- Same columns as the canonical `idx_orders_created_dt_hr` but under a
-- non-canonical name; drop so the canonical one gets created below.
CALL _drop_index_if_exists('shopify_orders', 'ix_so_created_dt_hr');
-- Single-column UTM indexes — superseded by `(created_dt, utm_*)` composites.
-- The pipeline always filters UTM together with a date range.
CALL _drop_index_if_exists('shopify_orders', 'idx_utm_source');
CALL _drop_index_if_exists('shopify_orders', 'idx_utm_medium');
CALL _drop_index_if_exists('shopify_orders', 'idx_utm_campaign');
CALL _drop_index_if_exists('shopify_orders', 'idx_utm_term');
CALL _drop_index_if_exists('shopify_orders', 'idx_utm_content');

-- Canonical index set: create any that are missing.
CALL _create_index_if_missing('shopify_orders', 'idx_updated_at',
    '`updated_at`');
CALL _create_index_if_missing('shopify_orders', 'idx_orders_created_dt_hr',
    '`created_dt`, `created_hr`');
CALL _create_index_if_missing('shopify_orders', 'idx_orders_created_dt_price',
    '`created_dt`, `total_price`');
CALL _create_index_if_missing('shopify_orders', 'idx_so_pg',
    '`payment_gateway_names`(50)');
CALL _create_index_if_missing('shopify_orders', 'ix_so_created_dt_app',
    '`created_dt`, `order_app_name`');
CALL _create_index_if_missing('shopify_orders', 'ix_so_order_id_created_dt',
    '`order_id`, `created_dt`');
CALL _create_index_if_missing('shopify_orders', 'idx_customer_created',
    '`customer_id`, `created_at`');
CALL _create_index_if_missing('shopify_orders', 'idx_so_created_dt_utm_source',
    '`created_dt`, `utm_source`(128)');
CALL _create_index_if_missing('shopify_orders', 'idx_so_created_dt_utm_medium',
    '`created_dt`, `utm_medium`(128)');
CALL _create_index_if_missing('shopify_orders', 'idx_so_created_dt_utm_campaign',
    '`created_dt`, `utm_campaign`(128)');
CALL _create_index_if_missing('shopify_orders', 'idx_created_product_qty',
    '`created_dt`, `product_id`, `line_item_quantity`');


-- ============================================================================
-- STEP 4: shopify_orders_update — converge to 4-index canonical set
-- ============================================================================

SELECT '=== STEP 4: shopify_orders_update indexes ===' AS section;

-- Drop obsolete varchar-based composites.
CALL _drop_index_if_exists('shopify_orders_update', 'idx_upd_dt_status_price');
CALL _drop_index_if_exists('shopify_orders_update', 'idx_sou_updated_date_status');
CALL _drop_index_if_exists('shopify_orders_update', 'idx_sou_updated_date_order');

-- Canonical set.
CALL _create_index_if_missing('shopify_orders_update', 'idx_updated_at',
    '`updated_at`');
CALL _create_index_if_missing('shopify_orders_update', 'idx_upd_order_name',
    '`order_name`');
CALL _create_index_if_missing('shopify_orders_update', 'idx_order_id',
    '`order_id`');
CALL _create_index_if_missing('shopify_orders_update', 'ix_sou_updated_dt',
    '`updated_dt`');


-- ============================================================================
-- STEP 5: Diagnostic — any indexes still present but NOT in canonical list
-- ============================================================================

SELECT '=== STEP 5: leftover (non-canonical) indexes — review manually ===' AS section;

SELECT
    TABLE_NAME,
    INDEX_NAME,
    GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS columns
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME IN ('shopify_orders', 'shopify_orders_update')
  AND INDEX_NAME <> 'PRIMARY'
  AND (TABLE_NAME, INDEX_NAME) NOT IN (
      ('shopify_orders', 'idx_updated_at'),
      ('shopify_orders', 'idx_orders_created_dt_hr'),
      ('shopify_orders', 'idx_orders_created_dt_price'),
      ('shopify_orders', 'idx_so_pg'),
      ('shopify_orders', 'ix_so_created_dt_app'),
      ('shopify_orders', 'ix_so_order_id_created_dt'),
      ('shopify_orders', 'idx_customer_created'),
      ('shopify_orders', 'idx_so_created_dt_utm_source'),
      ('shopify_orders', 'idx_so_created_dt_utm_medium'),
      ('shopify_orders', 'idx_so_created_dt_utm_campaign'),
      ('shopify_orders', 'idx_created_product_qty'),
      ('shopify_orders_update', 'idx_updated_at'),
      ('shopify_orders_update', 'idx_upd_order_name'),
      ('shopify_orders_update', 'idx_order_id'),
      ('shopify_orders_update', 'ix_sou_updated_dt')
  )
GROUP BY TABLE_NAME, INDEX_NAME
ORDER BY TABLE_NAME, INDEX_NAME;

-- If the result set above is empty, the database is fully converged.
-- If it has rows, inspect them — they are indexes added out-of-band on that
-- brand that the canonical set doesn't know about.


-- ============================================================================
-- CLEANUP: drop helper procedures
-- ============================================================================

DROP PROCEDURE IF EXISTS _drop_index_if_exists;
DROP PROCEDURE IF EXISTS _create_index_if_missing;
DROP PROCEDURE IF EXISTS _add_date_pk_if_missing;
DROP PROCEDURE IF EXISTS _add_date_hour_pk_if_missing;

SELECT '=== CONVERGE COMPLETE ===' AS section;
