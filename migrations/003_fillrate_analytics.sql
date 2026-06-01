CREATE TABLE IF NOT EXISTS product_fill_rate_daily (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    metric_date DATE NOT NULL,
    product_id BIGINT NOT NULL,
    variant_id BIGINT NULL,
    ordered_units INT NOT NULL DEFAULT 0,
    fulfilled_units INT NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_daily_product_variant (
        metric_date,
        product_id,
        variant_id
    ),
    KEY idx_product_date_variant (
        product_id,
        metric_date,
        variant_id
    ),
    KEY idx_metric_date (
        metric_date
    )
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS product_fill_rate_order_state (
    order_id VARCHAR(50) NOT NULL,
    metric_date DATE NOT NULL,
    product_id BIGINT NOT NULL,
    variant_id BIGINT NOT NULL DEFAULT 0,
    quantity INT NOT NULL DEFAULT 0,
    ordered_counted TINYINT(1) NOT NULL DEFAULT 0,
    fulfilled_counted TINYINT(1) NOT NULL DEFAULT 0,
    current_fulfillment_status VARCHAR(32) NOT NULL DEFAULT '',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (order_id, metric_date, product_id, variant_id),
    KEY idx_product_metric_date (
        product_id,
        metric_date,
        variant_id
    )
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS product_fill_rate_metrics (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    product_id BIGINT NOT NULL,
    variant_id BIGINT NULL,
    ordered_units_30d INT NOT NULL DEFAULT 0,
    fulfilled_units_30d INT NOT NULL DEFAULT 0,
    fill_rate_30d DECIMAL(10,2) NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_product_variant (
        product_id,
        variant_id
    ),
    KEY idx_product (
        product_id
    )
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
