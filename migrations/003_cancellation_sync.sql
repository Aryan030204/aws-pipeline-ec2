ALTER TABLE shopify_orders
    ADD COLUMN cancelled_at DATETIME NULL,
    ADD COLUMN cancel_reason VARCHAR(255) NULL;

ALTER TABLE returns_fact
    ADD COLUMN order_created_date DATE NULL AFTER event_type,
    ADD COLUMN cancelled_at DATETIME NULL AFTER order_created_date,
    ADD COLUMN cancel_reason VARCHAR(255) NULL AFTER cancelled_at;
