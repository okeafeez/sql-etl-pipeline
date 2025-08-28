-- ETL Transformation Queries
-- These queries are used by the ETL pipeline for data transformations

-- =====================================================
-- DIMENSION TABLE TRANSFORMATIONS
-- =====================================================

-- Transform and load customer dimension
INSERT INTO dw_analytics.dim_customers (
    customer_id, full_name, email, phone, address, city, state, 
    zip_code, country, customer_segment, registration_date, is_active
)
SELECT 
    customer_id,
    CONCAT(first_name, ' ', last_name) as full_name,
    LOWER(email) as email,
    REGEXP_REPLACE(phone, '[^0-9]', '', 'g') as phone,
    address,
    city,
    state,
    zip_code,
    country,
    customer_segment,
    registration_date::DATE,
    is_active
FROM staging.stg_customers
ON CONFLICT (customer_id) 
DO UPDATE SET
    full_name = EXCLUDED.full_name,
    email = EXCLUDED.email,
    phone = EXCLUDED.phone,
    address = EXCLUDED.address,
    city = EXCLUDED.city,
    state = EXCLUDED.state,
    zip_code = EXCLUDED.zip_code,
    customer_segment = EXCLUDED.customer_segment,
    is_active = EXCLUDED.is_active,
    updated_at = CURRENT_TIMESTAMP;

-- Transform and load product dimension
INSERT INTO dw_analytics.dim_products (
    product_id, product_name, category, subcategory, brand, 
    price, cost, profit_margin, weight, is_active
)
SELECT 
    product_id,
    INITCAP(product_name) as product_name,
    category,
    subcategory,
    brand,
    price,
    cost,
    CASE 
        WHEN price > 0 THEN ROUND(((price - cost) / price * 100), 2)
        ELSE 0
    END as profit_margin,
    weight,
    is_active
FROM staging.stg_products
ON CONFLICT (product_id)
DO UPDATE SET
    product_name = EXCLUDED.product_name,
    category = EXCLUDED.category,
    subcategory = EXCLUDED.subcategory,
    brand = EXCLUDED.brand,
    price = EXCLUDED.price,
    cost = EXCLUDED.cost,
    profit_margin = EXCLUDED.profit_margin,
    weight = EXCLUDED.weight,
    is_active = EXCLUDED.is_active,
    updated_at = CURRENT_TIMESTAMP;

-- Populate date dimension (if not already populated)
INSERT INTO dw_analytics.dim_date (
    date_key, full_date, year, quarter, month, month_name, 
    week, day_of_year, day_of_month, day_of_week, day_name, 
    is_weekend, is_holiday
)
SELECT 
    TO_CHAR(date_series, 'YYYYMMDD')::INTEGER as date_key,
    date_series as full_date,
    EXTRACT(YEAR FROM date_series) as year,
    EXTRACT(QUARTER FROM date_series) as quarter,
    EXTRACT(MONTH FROM date_series) as month,
    TO_CHAR(date_series, 'Month') as month_name,
    EXTRACT(WEEK FROM date_series) as week,
    EXTRACT(DOY FROM date_series) as day_of_year,
    EXTRACT(DAY FROM date_series) as day_of_month,
    EXTRACT(DOW FROM date_series) as day_of_week,
    TO_CHAR(date_series, 'Day') as day_name,
    CASE WHEN EXTRACT(DOW FROM date_series) IN (0, 6) THEN TRUE ELSE FALSE END as is_weekend,
    FALSE as is_holiday  -- This could be enhanced with actual holiday data
FROM generate_series(
    '2020-01-01'::DATE, 
    '2030-12-31'::DATE, 
    '1 day'::INTERVAL
) as date_series
ON CONFLICT (date_key) DO NOTHING;

-- =====================================================
-- FACT TABLE TRANSFORMATIONS
-- =====================================================

-- Transform and load sales fact table
WITH order_details AS (
    SELECT 
        o.order_id,
        o.customer_id,
        o.order_date,
        oi.product_id,
        oi.quantity,
        oi.unit_price,
        oi.total_price,
        oi.discount_applied,
        o.tax_amount,
        o.shipping_cost,
        -- Calculate proportional tax and shipping for each line item
        CASE 
            WHEN SUM(oi.total_price) OVER (PARTITION BY o.order_id) > 0 THEN
                o.tax_amount * (oi.total_price / SUM(oi.total_price) OVER (PARTITION BY o.order_id))
            ELSE 0
        END as line_tax_amount,
        CASE 
            WHEN COUNT(*) OVER (PARTITION BY o.order_id) > 0 THEN
                o.shipping_cost / COUNT(*) OVER (PARTITION BY o.order_id)
            ELSE 0
        END as line_shipping_cost
    FROM staging.stg_orders o
    JOIN source_ecommerce.order_items oi ON o.order_id = oi.order_id
    WHERE o.order_status NOT IN ('Cancelled')
)
INSERT INTO dw_analytics.fact_sales (
    order_id, customer_key, product_key, order_date_key,
    quantity, unit_price, total_sales, total_cost, profit,
    discount_amount, tax_amount, shipping_cost
)
SELECT 
    od.order_id,
    dc.customer_key,
    dp.product_key,
    TO_CHAR(od.order_date, 'YYYYMMDD')::INTEGER as order_date_key,
    od.quantity,
    od.unit_price,
    od.total_price as total_sales,
    od.quantity * dp.cost as total_cost,
    od.total_price - (od.quantity * dp.cost) as profit,
    od.discount_applied,
    od.line_tax_amount,
    od.line_shipping_cost
FROM order_details od
JOIN dw_analytics.dim_customers dc ON od.customer_id = dc.customer_id
JOIN dw_analytics.dim_products dp ON od.product_id = dp.product_id
ON CONFLICT (order_id, product_key) 
DO UPDATE SET
    quantity = EXCLUDED.quantity,
    unit_price = EXCLUDED.unit_price,
    total_sales = EXCLUDED.total_sales,
    total_cost = EXCLUDED.total_cost,
    profit = EXCLUDED.profit,
    discount_amount = EXCLUDED.discount_amount,
    tax_amount = EXCLUDED.tax_amount,
    shipping_cost = EXCLUDED.shipping_cost,
    created_at = CURRENT_TIMESTAMP;

-- Transform and load inventory fact table
INSERT INTO dw_analytics.fact_inventory (
    product_key, snapshot_date_key, warehouse_location,
    quantity_on_hand, quantity_reserved, available_quantity,
    reorder_level, stock_status
)
SELECT 
    dp.product_key,
    TO_CHAR(CURRENT_DATE, 'YYYYMMDD')::INTEGER as snapshot_date_key,
    i.warehouse_location,
    i.quantity_on_hand,
    i.quantity_reserved,
    i.quantity_on_hand - i.quantity_reserved as available_quantity,
    i.reorder_level,
    CASE 
        WHEN i.quantity_on_hand - i.quantity_reserved <= 0 THEN 'Out of Stock'
        WHEN i.quantity_on_hand - i.quantity_reserved <= i.reorder_level THEN 'Low Stock'
        ELSE 'In Stock'
    END as stock_status
FROM source_ecommerce.inventory i
JOIN dw_analytics.dim_products dp ON i.product_id = dp.product_id
ON CONFLICT (product_key, snapshot_date_key, warehouse_location)
DO UPDATE SET
    quantity_on_hand = EXCLUDED.quantity_on_hand,
    quantity_reserved = EXCLUDED.quantity_reserved,
    available_quantity = EXCLUDED.available_quantity,
    reorder_level = EXCLUDED.reorder_level,
    stock_status = EXCLUDED.stock_status,
    created_at = CURRENT_TIMESTAMP;

-- =====================================================
-- DATA QUALITY AND VALIDATION QUERIES
-- =====================================================

-- Data quality check: Orphaned records
SELECT 'Orphaned Order Items' as check_name, COUNT(*) as issue_count
FROM source_ecommerce.order_items oi
LEFT JOIN source_ecommerce.orders o ON oi.order_id = o.order_id
WHERE o.order_id IS NULL

UNION ALL

SELECT 'Orphaned Orders' as check_name, COUNT(*) as issue_count
FROM source_ecommerce.orders o
LEFT JOIN source_ecommerce.customers c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL

UNION ALL

SELECT 'Invalid Product Prices' as check_name, COUNT(*) as issue_count
FROM source_ecommerce.products
WHERE price <= 0 OR cost < 0 OR price < cost

UNION ALL

SELECT 'Negative Inventory' as check_name, COUNT(*) as issue_count
FROM source_ecommerce.inventory
WHERE quantity_on_hand < 0 OR quantity_reserved < 0

UNION ALL

SELECT 'Future Order Dates' as check_name, COUNT(*) as issue_count
FROM source_ecommerce.orders
WHERE order_date > CURRENT_TIMESTAMP;

-- =====================================================
-- INCREMENTAL LOAD QUERIES
-- =====================================================

-- Get last processed timestamp for incremental loads
CREATE OR REPLACE FUNCTION get_last_processed_timestamp(table_name TEXT)
RETURNS TIMESTAMP AS $$
DECLARE
    last_timestamp TIMESTAMP;
BEGIN
    -- This would typically query a control table
    -- For demo purposes, we'll use a simple approach
    CASE table_name
        WHEN 'customers' THEN
            SELECT MAX(updated_at) INTO last_timestamp 
            FROM dw_analytics.dim_customers;
        WHEN 'products' THEN
            SELECT MAX(updated_at) INTO last_timestamp 
            FROM dw_analytics.dim_products;
        WHEN 'orders' THEN
            SELECT MAX(created_at) INTO last_timestamp 
            FROM dw_analytics.fact_sales;
        ELSE
            last_timestamp := '1900-01-01'::TIMESTAMP;
    END CASE;
    
    RETURN COALESCE(last_timestamp, '1900-01-01'::TIMESTAMP);
END;
$$ LANGUAGE plpgsql;

-- Incremental customer extraction
SELECT *
FROM source_ecommerce.customers
WHERE updated_date > get_last_processed_timestamp('customers')
   OR (updated_date IS NULL AND registration_date > get_last_processed_timestamp('customers'));

-- Incremental product extraction
SELECT *
FROM source_ecommerce.products
WHERE updated_date > get_last_processed_timestamp('products');

-- Incremental order extraction
SELECT *
FROM source_ecommerce.orders
WHERE order_date > get_last_processed_timestamp('orders');

-- =====================================================
-- PERFORMANCE OPTIMIZATION QUERIES
-- =====================================================

-- Create materialized views for frequently accessed aggregations
CREATE MATERIALIZED VIEW IF NOT EXISTS dw_analytics.mv_monthly_sales AS
SELECT 
    DATE_TRUNC('month', dd.full_date) as month,
    dc.customer_segment,
    dp.category,
    COUNT(DISTINCT fs.order_id) as order_count,
    COUNT(DISTINCT fs.customer_key) as unique_customers,
    SUM(fs.quantity) as total_quantity,
    SUM(fs.total_sales) as total_revenue,
    SUM(fs.profit) as total_profit,
    AVG(fs.total_sales) as avg_order_value
FROM dw_analytics.fact_sales fs
JOIN dw_analytics.dim_date dd ON fs.order_date_key = dd.date_key
JOIN dw_analytics.dim_customers dc ON fs.customer_key = dc.customer_key
JOIN dw_analytics.dim_products dp ON fs.product_key = dp.product_key
GROUP BY DATE_TRUNC('month', dd.full_date), dc.customer_segment, dp.category;

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_fact_sales_date_key ON dw_analytics.fact_sales(order_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer_key ON dw_analytics.fact_sales(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product_key ON dw_analytics.fact_sales(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_inventory_date_key ON dw_analytics.fact_inventory(snapshot_date_key);
CREATE INDEX IF NOT EXISTS idx_dim_date_full_date ON dw_analytics.dim_date(full_date);

-- Refresh materialized view (to be run periodically)
REFRESH MATERIALIZED VIEW dw_analytics.mv_monthly_sales;

-- =====================================================
-- ETL MONITORING AND LOGGING
-- =====================================================

-- Create ETL log table for tracking pipeline runs
CREATE TABLE IF NOT EXISTS dw_analytics.etl_log (
    log_id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(100),
    table_name VARCHAR(100),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status VARCHAR(20),
    records_processed INTEGER,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Function to log ETL pipeline execution
CREATE OR REPLACE FUNCTION log_etl_execution(
    p_pipeline_name TEXT,
    p_table_name TEXT,
    p_start_time TIMESTAMP,
    p_end_time TIMESTAMP,
    p_status TEXT,
    p_records_processed INTEGER DEFAULT 0,
    p_error_message TEXT DEFAULT NULL
)
RETURNS VOID AS $$
BEGIN
    INSERT INTO dw_analytics.etl_log (
        pipeline_name, table_name, start_time, end_time, 
        status, records_processed, error_message
    )
    VALUES (
        p_pipeline_name, p_table_name, p_start_time, p_end_time,
        p_status, p_records_processed, p_error_message
    );
END;
$$ LANGUAGE plpgsql;

-- Query to monitor ETL pipeline performance
SELECT 
    pipeline_name,
    table_name,
    COUNT(*) as total_runs,
    COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) as successful_runs,
    COUNT(CASE WHEN status = 'FAILED' THEN 1 END) as failed_runs,
    AVG(EXTRACT(EPOCH FROM (end_time - start_time))) as avg_duration_seconds,
    MAX(end_time) as last_run_time,
    SUM(records_processed) as total_records_processed
FROM dw_analytics.etl_log
WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY pipeline_name, table_name
ORDER BY pipeline_name, table_name;

