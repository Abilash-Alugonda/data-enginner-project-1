-- Migration 001: Core warehouse schema and dimension tables
CREATE TABLE IF NOT EXISTS schema_version (
    version VARCHAR PRIMARY KEY,
    applied_at VARCHAR NOT NULL,
    description VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    email VARCHAR NOT NULL,
    country VARCHAR NOT NULL,
    created_at VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS products (
    product_id VARCHAR PRIMARY KEY,
    product_name VARCHAR NOT NULL,
    category VARCHAR NOT NULL,
    unit_price DOUBLE NOT NULL
);

CREATE TABLE IF NOT EXISTS orders_staging (
    order_id VARCHAR PRIMARY KEY,
    customer_id VARCHAR NOT NULL,
    product_id VARCHAR NOT NULL,
    quantity BIGINT NOT NULL,
    order_amount DOUBLE NOT NULL,
    order_status VARCHAR NOT NULL,
    order_date VARCHAR NOT NULL,
    updated_at VARCHAR NOT NULL,
    run_id VARCHAR NOT NULL,
    source_file VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR PRIMARY KEY,
    customer_id VARCHAR NOT NULL,
    product_id VARCHAR NOT NULL,
    quantity BIGINT NOT NULL,
    order_amount DOUBLE NOT NULL,
    order_status VARCHAR NOT NULL,
    order_date VARCHAR NOT NULL,
    updated_at VARCHAR NOT NULL,
    run_id VARCHAR NOT NULL,
    source_file VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS orders_full_refresh (
    order_id VARCHAR PRIMARY KEY,
    customer_id VARCHAR NOT NULL,
    product_id VARCHAR NOT NULL,
    quantity BIGINT NOT NULL,
    order_amount DOUBLE NOT NULL,
    order_status VARCHAR NOT NULL,
    order_date VARCHAR NOT NULL,
    updated_at VARCHAR NOT NULL,
    run_id VARCHAR NOT NULL,
    source_file VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS orders_incremental (
    order_id VARCHAR PRIMARY KEY,
    customer_id VARCHAR NOT NULL,
    product_id VARCHAR NOT NULL,
    quantity BIGINT NOT NULL,
    order_amount DOUBLE NOT NULL,
    order_status VARCHAR NOT NULL,
    order_date VARCHAR NOT NULL,
    updated_at VARCHAR NOT NULL,
    run_id VARCHAR NOT NULL,
    source_file VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS orders_partition_index (
    partition_key VARCHAR NOT NULL,
    partition_date VARCHAR,
    file_path VARCHAR NOT NULL,
    row_count BIGINT NOT NULL,
    record_count BIGINT,
    updated_at VARCHAR NOT NULL,
    last_updated_at VARCHAR,
    run_id VARCHAR NOT NULL,
    PRIMARY KEY (partition_key, file_path)
);

CREATE TABLE IF NOT EXISTS watermarks (
    strategy VARCHAR PRIMARY KEY,
    last_watermark VARCHAR NOT NULL,
    updated_at VARCHAR NOT NULL
);

