-- migrate: manual
-- =============================================================================
-- RDS Performance Pass #1
-- Goal: stop EBSByteBalance% from hitting 0 on the per-brand MySQL.
--
-- ⚠️  SUPERSEDED, AND NOT SAFE TO RE-RUN. Use 002_converge_to_canonical.sql
--     instead: it performs this same work through information_schema-guarded
--     stored procedures, so it is genuinely idempotent and is what the
--     automatic runner (pipeline/sql_migrations.py) applies.
--
--     This file is kept for provenance and is marked `-- migrate: manual`
--     above, which makes the runner list it and skip it. Do not remove that
--     marker. Reasons it cannot be automated:
--
--       * The `ADD PRIMARY KEY` statements below fail with errno 1068
--         ("Multiple primary key defined") the second time they run - there
--         is no IF NOT EXISTS form for a primary key.
--       * The `DROP INDEX` statements fail with errno 1091 ("Can't DROP ...
--         check that column/key exists") when the index is already gone, and
--         because each ALTER drops several indexes in one statement, one
--         already-dropped index aborts the whole batch.
--       * The bare SELECT COUNT(*) pre-flight checks are output for a human
--         to READ AND ACT ON before running the ALTER that follows them;
--         nothing enforces them. A runner would happily execute the ALTER
--         over duplicate or NULL dates and fail on a half-rebuilt table.
--       * ALGORITHM=COPY rewrites the table, and the shopify_orders phases
--         want a maintenance window - see the phase notes below.
--
-- Run this PER BRAND DATABASE (each logical tenant), one phase per
-- maintenance window; do not run all phases at once on a hot database.
--
-- Phases:
--   1. Add PK/date indexes to the summary tables (cheap, massive read win).
--   2. Drop redundant composite indexes on shopify_orders / _update
--      (frees write bandwidth on every insert and update).
--   3. Add a primary key to shopify_orders / _update
--      (biggest single win, but requires a window — schedule separately).
--
-- The ETL pipeline (aws_background.py) has been patched to use `created_dt`
-- (the existing generated DATE column) and `created_hr` instead of the
-- VARCHAR `created_date` + `HOUR(created_time)` functions, so the varchar
-- indexes we drop below are no longer read by the pipeline. Verify your
-- dashboard service (see DATABASE_QUERIES.md) has been migrated before
-- dropping `idx_so_created_date_time` / `idx_so_created_date_order` /
-- `idx_created_date_totalprice`.
--
-- How to run safely on RDS MySQL 8:
--   * All ALTERs below use ALGORITHM=INPLACE, LOCK=NONE where the server
--     supports it. For adding a PK, online DDL is only supported when the
--     PK column is NOT NULL and unique; we pre-check that first.
--   * Use `pt-online-schema-change` or `gh-ost` if the tables are very
--     large (>10M rows) and you cannot tolerate any extra undo/redo load.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- PHASE 1: Summary tables — add primary key on `date`
--
-- These tables are rewritten per day by the pipeline and read per day by the
-- dashboard. Every query on them today is a full scan because there is no
-- index on `date`. A PK fixes that and also allows REPLACE INTO semantics
-- (used by the patched pipeline).
-- -----------------------------------------------------------------------------

-- Note on ALGORITHM: converting a nullable column to NOT NULL under strict
-- sql_mode requires ALGORITHM=COPY (the server rebuilds the table so it can
-- verify no NULL rows slip in). Summary tables are tiny (one row per day,
-- single-digit MB), so COPY finishes in seconds. For large production tables
-- you would instead use gh-ost / pt-online-schema-change.
--
-- Pre-flight for each table: make sure no NULL dates exist and no duplicate
-- dates exist (a crashed ETL could have left both). If any of these return
-- non-zero, resolve the rows (DELETE or re-run the ETL to regenerate them)
-- before running the ALTER that follows.

-- ── overall_summary ─────────────────────────────────────────────────────────
SELECT COUNT(*) AS null_dates   FROM overall_summary WHERE `date` IS NULL;
SELECT COUNT(*) AS dup_dates    FROM (SELECT `date` FROM overall_summary GROUP BY `date` HAVING COUNT(*) > 1) t;
-- If dup_dates > 0, keep the latest row per date:
--   DELETE a FROM overall_summary a
--   JOIN overall_summary b ON a.`date` = b.`date`
--   WHERE a.`date` IS NOT NULL AND b.`date` IS NOT NULL AND <ordering key picks loser>;
-- If null_dates > 0:
--   DELETE FROM overall_summary WHERE `date` IS NULL;

ALTER TABLE overall_summary
    MODIFY `date` DATE NOT NULL,
    ADD PRIMARY KEY (`date`),
    ALGORITHM=COPY, LOCK=SHARED;

-- ── sales_summary ───────────────────────────────────────────────────────────
SELECT COUNT(*) AS null_dates   FROM sales_summary WHERE `date` IS NULL;
SELECT COUNT(*) AS dup_dates    FROM (SELECT `date` FROM sales_summary GROUP BY `date` HAVING COUNT(*) > 1) t;
ALTER TABLE sales_summary
    MODIFY `date` DATE NOT NULL,
    ADD PRIMARY KEY (`date`),
    ALGORITHM=COPY, LOCK=SHARED;

-- ── gross_summary ───────────────────────────────────────────────────────────
SELECT COUNT(*) AS null_dates   FROM gross_summary WHERE `date` IS NULL;
SELECT COUNT(*) AS dup_dates    FROM (SELECT `date` FROM gross_summary GROUP BY `date` HAVING COUNT(*) > 1) t;
ALTER TABLE gross_summary
    MODIFY `date` DATE NOT NULL,
    ADD PRIMARY KEY (`date`),
    ALGORITHM=COPY, LOCK=SHARED;

-- ── discount_summary ────────────────────────────────────────────────────────
SELECT COUNT(*) AS null_dates   FROM discount_summary WHERE `date` IS NULL;
SELECT COUNT(*) AS dup_dates    FROM (SELECT `date` FROM discount_summary GROUP BY `date` HAVING COUNT(*) > 1) t;
ALTER TABLE discount_summary
    MODIFY `date` DATE NOT NULL,
    ADD PRIMARY KEY (`date`),
    ALGORITHM=COPY, LOCK=SHARED;

-- ── order_summary ───────────────────────────────────────────────────────────
SELECT COUNT(*) AS null_dates   FROM order_summary WHERE `date` IS NULL;
SELECT COUNT(*) AS dup_dates    FROM (SELECT `date` FROM order_summary GROUP BY `date` HAVING COUNT(*) > 1) t;
ALTER TABLE order_summary
    MODIFY `date` DATE NOT NULL,
    ADD PRIMARY KEY (`date`),
    ALGORITHM=COPY, LOCK=SHARED;

-- ── hour_wise_sales: composite PK on (date, hour) ───────────────────────────
SELECT COUNT(*) AS null_keys    FROM hour_wise_sales WHERE `date` IS NULL OR `hour` IS NULL;
SELECT COUNT(*) AS dup_keys     FROM (SELECT `date`, `hour` FROM hour_wise_sales GROUP BY `date`, `hour` HAVING COUNT(*) > 1) t;
ALTER TABLE hour_wise_sales
    MODIFY `date` DATE NOT NULL,
    MODIFY `hour` INT NOT NULL,
    ADD PRIMARY KEY (`date`, `hour`),
    ALGORITHM=COPY, LOCK=SHARED;

-- ── *_stage tables ──────────────────────────────────────────────────────────
-- These are TRUNCATE'd at the start of every incremental run, so they should
-- be empty or near-empty when the migration runs — no pre-flight needed.
ALTER TABLE overall_summary_stage
    MODIFY `date` DATE NOT NULL, ADD PRIMARY KEY (`date`),
    ALGORITHM=COPY, LOCK=SHARED;
ALTER TABLE sales_summary_stage
    MODIFY `date` DATE NOT NULL, ADD PRIMARY KEY (`date`),
    ALGORITHM=COPY, LOCK=SHARED;
ALTER TABLE gross_summary_stage
    MODIFY `date` DATE NOT NULL, ADD PRIMARY KEY (`date`),
    ALGORITHM=COPY, LOCK=SHARED;
ALTER TABLE discount_summary_stage
    MODIFY `date` DATE NOT NULL, ADD PRIMARY KEY (`date`),
    ALGORITHM=COPY, LOCK=SHARED;
ALTER TABLE order_summary_stage
    MODIFY `date` DATE NOT NULL, ADD PRIMARY KEY (`date`),
    ALGORITHM=COPY, LOCK=SHARED;
ALTER TABLE hour_wise_sales_stage
    MODIFY `date` DATE NOT NULL, MODIFY `hour` INT NOT NULL,
    ADD PRIMARY KEY (`date`, `hour`),
    ALGORITHM=COPY, LOCK=SHARED;

-- returns_fact already has a PK (order_id, event_type, event_date) — leave it.
-- sessions_summary already has PK(date), leave it.


-- -----------------------------------------------------------------------------
-- PHASE 2: shopify_orders — drop redundant indexes
--
-- Live list (12 indexes today). Every INSERT touches all of them, which is
-- the #1 write amplifier driving EBSByteBalance% to 0.
--
-- KEEP:
--   * idx_updated_at                 — incremental fetch high-water mark
--   * idx_orders_created_dt_hr       — pipeline GROUP BY (created_dt, created_hr)
--   * ix_so_created_dt_app           — per-app sales aggregation
--   * ix_so_order_id_created_dt      — order_id lookups + date-range tiebreak
--   * idx_so_pg                      — COD/Prepaid classification (payment_gateway_names prefix)
--   * idx_created_product_qty        — products_sold_units_daily upsert path
--
-- DROP (all superseded or unused after the pipeline patch):
--   * idx_created_date_totalprice    — varchar created_date, now replaced by created_dt access
--   * idx_so_created_date_time       — varchar
--   * idx_so_created_date_order      — varchar
--   * idx_created_at_order_id        — (created_at, order_id) is covered by ix_so_order_id_created_dt + date is now via created_dt
--   * idx_order_app_name             — single-column, cardinality=4; fully shadowed by ix_so_created_dt_app
--
-- Verify EXPLAIN on your heaviest dashboard queries before dropping. Index
-- drops are cheap to reverse if a regression shows up.
-- -----------------------------------------------------------------------------

ALTER TABLE shopify_orders
    DROP INDEX idx_created_date_totalprice,
    DROP INDEX idx_so_created_date_time,
    DROP INDEX idx_so_created_date_order,
    DROP INDEX idx_created_at_order_id,
    DROP INDEX idx_order_app_name,
    ALGORITHM=INPLACE, LOCK=NONE;

-- Optional second wave — only if EXPLAIN on your heaviest queries confirms it
-- is never chosen. `total_price` is almost never in a WHERE clause in the
-- pipeline or dashboard, so idx_orders_created_dt_price is very likely dead
-- weight, but verify before dropping:
--
--   EXPLAIN SELECT ... FROM shopify_orders
--   WHERE created_dt BETWEEN '2025-01-01' AND '2025-01-31' AND total_price > 0;
--
-- If `possible_keys` does not list it, drop:
-- ALTER TABLE shopify_orders
--     DROP INDEX idx_orders_created_dt_price,
--     ALGORITHM=INPLACE, LOCK=NONE;


-- -----------------------------------------------------------------------------
-- PHASE 2b: additional redundant indexes found by SHOW INDEX on live DB
--
-- After running Phase 2, `SHOW INDEX FROM shopify_orders` revealed several
-- more indexes that were not in the schema dump originally reviewed. These
-- fall into three buckets:
--
--   (a) varchar `created_date` duplicates of proper `created_dt` versions
--       (same columns, wrong base type — the pipeline patch uses the
--       `created_dt` versions exclusively):
--         idx_so_created_date_utm_source      → idx_so_created_dt_utm_source
--         idx_so_created_date_utm_medium      → idx_so_created_dt_utm_medium
--         idx_so_created_date_utm_campaign    → idx_so_created_dt_utm_campaign
--         idx_so_created_date_order_app       → ix_so_created_dt_app
--
--   (b) exact duplicate of an existing index (same columns, same order):
--         idx_so_created_dt_order_app         == ix_so_created_dt_app
--
--   (c) product-level composites with a varchar middle column (breaks range
--       usage) or typoed names, fully shadowed by idx_created_product_qty:
--         idx_product_created_qty             (product_id, created_date, line_item_quantity)
--         idx_created_product_linte_item_qty  (created_date, product_id, line_item_quantity)
--
-- Net effect after this block: 18 → 10 indexes. Each dropped index saves one
-- B-tree write per INSERT and one entry of buffer-pool pressure.
-- -----------------------------------------------------------------------------

ALTER TABLE shopify_orders
    DROP INDEX idx_so_created_date_utm_source,
    DROP INDEX idx_so_created_date_utm_medium,
    DROP INDEX idx_so_created_date_utm_campaign,
    DROP INDEX idx_so_created_date_order_app,
    DROP INDEX idx_so_created_dt_order_app,
    DROP INDEX idx_product_created_qty,
    DROP INDEX idx_created_product_linte_item_qty,
    ALGORITHM=INPLACE, LOCK=NONE;

-- Judgement call — drop if nothing outside the ETL/dashboard uses it:
--   `idx_customer_created` (customer_id, created_at). Not referenced by the
--   pipeline or by the dashboard queries in DATABASE_QUERIES.md. If you have
--   manual RFM/repeat-customer queries that filter by customer_id, keep it.
--
-- To check if it has been used recently (MySQL 8.0+):
--   SELECT index_name, count_read, count_write
--   FROM performance_schema.table_io_waits_summary_by_index_usage
--   WHERE object_schema = DATABASE() AND object_name = 'shopify_orders';
--
-- If count_read = 0 for idx_customer_created:
-- ALTER TABLE shopify_orders
--     DROP INDEX idx_customer_created,
--     ALGORITHM=INPLACE, LOCK=NONE;

-- shopify_orders_update: same treatment. It carries the incremental fetch
-- output and is rewritten constantly, so redundant indexes hurt the most.
ALTER TABLE shopify_orders_update
    DROP INDEX idx_upd_dt_status_price,
    DROP INDEX idx_sou_updated_date_status,
    DROP INDEX idx_sou_updated_date_order,
    ALGORITHM=INPLACE, LOCK=NONE;


-- -----------------------------------------------------------------------------
-- PHASE 3: shopify_orders / _update — add a primary key
--
-- Today the table has no PK, so every secondary index stores the full
-- invisible DB_ROW_ID and every insert causes a random heap write. Adding a
-- PK gives us clustered-index ordering on the natural access path and
-- dramatically lowers write amplification.
--
-- Choice of PK: the natural unique key per row is (order_id, line_item) —
-- each Shopify order has one line per variant ordered. Before running, VERIFY:
--
--   SELECT order_id, line_item, COUNT(*) c
--   FROM shopify_orders
--   GROUP BY order_id, line_item
--   HAVING c > 1
--   LIMIT 10;
--
-- If this returns rows, the natural key has duplicates and you need a
-- surrogate PK instead:
--
--   ALTER TABLE shopify_orders ADD COLUMN id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST;
--
-- If the natural key is clean, the preferred form is below. It is NOT
-- online DDL (adding a PK rebuilds the table); schedule a maintenance
-- window and use gh-ost / pt-online-schema-change for large brands.
-- -----------------------------------------------------------------------------

-- Only run this once per brand after verifying the natural key is unique:
-- ALTER TABLE shopify_orders
--     MODIFY order_id VARCHAR(50) NOT NULL,
--     MODIFY line_item VARCHAR(255) NOT NULL,
--     ADD PRIMARY KEY (order_id, line_item);

-- Likewise for shopify_orders_update (if that table also has no PK):
-- ALTER TABLE shopify_orders_update
--     MODIFY order_id VARCHAR(50) NOT NULL,
--     MODIFY line_item VARCHAR(255) NOT NULL,
--     ADD PRIMARY KEY (order_id, line_item);


-- -----------------------------------------------------------------------------
-- PHASE 4: Small targeted indexes for the patched pipeline
-- -----------------------------------------------------------------------------

-- The patched pipeline aggregates orders by (created_dt, created_hr). The
-- existing idx_orders_created_dt_hr already covers this — no new index needed.

-- The dashboard's LEFT JOIN chain on summary tables now has a PK on `date`,
-- so no further index additions are required there either.

-- If you kept any brand on the old varchar path temporarily, you can add:
--   ALTER TABLE shopify_orders ADD INDEX idx_created_dt (created_dt);
-- to cover bare `WHERE created_dt BETWEEN ...` without order_app_name.
