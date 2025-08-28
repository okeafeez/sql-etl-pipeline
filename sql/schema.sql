-- SQL ETL Pipeline Database Schema
-- This file contains the schema definitions for both source and target systems

-- =====================================================
-- SOURCE SYSTEM SCHEMAS
-- =====================================================

-- Create source database schema for an e-commerce system
CREATE SCHEMA IF NOT EXISTS source_ecommerce;

-- Customers table (source)
CREATE TABLE IF NOT EXISTS source_ecommerce.customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(20),
    address TEXT,
    city VARCHAR(50),
    state VARCHAR(50),
    zip_code VARCHAR(10),
    country VARCHAR(50) DEFAULT 'USA',
    registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    customer_segment VARCHAR(20) DEFAULT 'Regular'
);

-- Products table (source)
CREATE TABLE IF NOT EXISTS source_ecommerce.products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(200) NOT NULL,
    category VARCHAR(50),
    subcategory VARCHAR(50),
    brand VARCHAR(50),
    price DECIMAL(10,2) NOT NULL,
    cost DECIMAL(10,2),
    weight DECIMAL(8,2),
    dimensions VARCHAR(50),
    description TEXT,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

-- Orders table (source)
CREATE TABLE IF NOT EXISTS source_ecommerce.orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES source_ecommerce.customers(customer_id),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    order_status VARCHAR(20) DEFAULT 'Pending',
    shipping_address TEXT,
    billing_address TEXT,
    payment_method VARCHAR(30),
    shipping_method VARCHAR(30),
    order_total DECIMAL(12,2),
    tax_amount DECIMAL(10,2),
    shipping_cost DECIMAL(8,2),
    discount_amount DECIMAL(8,2) DEFAULT 0,
    notes TEXT
);

-- Order Items table (source)
CREATE TABLE IF NOT EXISTS source_ecommerce.order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES source_ecommerce.orders(order_id),
    product_id INTEGER REFERENCES source_ecommerce.products(product_id),
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    total_price DECIMAL(12,2) NOT NULL,
    discount_applied DECIMAL(8,2) DEFAULT 0
);

-- Inventory table (source)
CREATE TABLE IF NOT EXISTS source_ecommerce.inventory (
    inventory_id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES source_ecommerce.products(product_id),
    warehouse_location VARCHAR(50),
    quantity_on_hand INTEGER DEFAULT 0,
    quantity_reserved INTEGER DEFAULT 0,
    reorder_level INTEGER DEFAULT 10,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- TARGET SYSTEM SCHEMAS (Data Warehouse)
-- =====================================================

-- Create target data warehouse schema
CREATE SCHEMA IF NOT EXISTS dw_analytics;

-- Dimension Tables

-- Customer Dimension
CREATE TABLE IF NOT EXISTS dw_analytics.dim_customers (
    customer_key SERIAL PRIMARY KEY,
    customer_id INTEGER UNIQUE NOT NULL,
    full_name VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(20),
    address TEXT,
    city VARCHAR(50),
    state VARCHAR(50),
    zip_code VARCHAR(10),
    country VARCHAR(50),
    customer_segment VARCHAR(20),
    registration_date DATE,
    is_active BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Product Dimension
CREATE TABLE IF NOT EXISTS dw_analytics.dim_products (
    product_key SERIAL PRIMARY KEY,
    product_id INTEGER UNIQUE NOT NULL,
    product_name VARCHAR(200),
    category VARCHAR(50),
    subcategory VARCHAR(50),
    brand VARCHAR(50),
    price DECIMAL(10,2),
    cost DECIMAL(10,2),
    profit_margin DECIMAL(5,2),
    weight DECIMAL(8,2),
    is_active BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Date Dimension
CREATE TABLE IF NOT EXISTS dw_analytics.dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE UNIQUE NOT NULL,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    month_name VARCHAR(20),
    week INTEGER,
    day_of_year INTEGER,
    day_of_month INTEGER,
    day_of_week INTEGER,
    day_name VARCHAR(20),
    is_weekend BOOLEAN,
    is_holiday BOOLEAN
);

-- Fact Tables

-- Sales Fact Table
CREATE TABLE IF NOT EXISTS dw_analytics.fact_sales (
    sales_key SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL,
    customer_key INTEGER REFERENCES dw_analytics.dim_customers(customer_key),
    product_key INTEGER REFERENCES dw_analytics.dim_products(product_key),
    order_date_key INTEGER REFERENCES dw_analytics.dim_date(date_key),
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    total_sales DECIMAL(12,2),
    total_cost DECIMAL(12,2),
    profit DECIMAL(12,2),
    discount_amount DECIMAL(8,2),
    tax_amount DECIMAL(10,2),
    shipping_cost DECIMAL(8,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Inventory Fact Table
CREATE TABLE IF NOT EXISTS dw_analytics.fact_inventory (
    inventory_key SERIAL PRIMARY KEY,
    product_key INTEGER REFERENCES dw_analytics.dim_products(product_key),
    snapshot_date_key INTEGER REFERENCES dw_analytics.dim_date(date_key),
    warehouse_location VARCHAR(50),
    quantity_on_hand INTEGER,
    quantity_reserved INTEGER,
    available_quantity INTEGER,
    reorder_level INTEGER,
    stock_status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- STAGING TABLES
-- =====================================================

-- Create staging schema for intermediate processing
CREATE SCHEMA IF NOT EXISTS staging;

-- Staging table for customer data
CREATE TABLE IF NOT EXISTS staging.stg_customers (
    customer_id INTEGER,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    phone VARCHAR(20),
    address TEXT,
    city VARCHAR(50),
    state VARCHAR(50),
    zip_code VARCHAR(10),
    country VARCHAR(50),
    registration_date TIMESTAMP,
    last_login TIMESTAMP,
    is_active BOOLEAN,
    customer_segment VARCHAR(20),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Staging table for product data
CREATE TABLE IF NOT EXISTS staging.stg_products (
    product_id INTEGER,
    product_name VARCHAR(200),
    category VARCHAR(50),
    subcategory VARCHAR(50),
    brand VARCHAR(50),
    price DECIMAL(10,2),
    cost DECIMAL(10,2),
    weight DECIMAL(8,2),
    dimensions VARCHAR(50),
    description TEXT,
    created_date TIMESTAMP,
    updated_date TIMESTAMP,
    is_active BOOLEAN,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Staging table for order data
CREATE TABLE IF NOT EXISTS staging.stg_orders (
    order_id INTEGER,
    customer_id INTEGER,
    order_date TIMESTAMP,
    order_status VARCHAR(20),
    shipping_address TEXT,
    billing_address TEXT,
    payment_method VARCHAR(30),
    shipping_method VARCHAR(30),
    order_total DECIMAL(12,2),
    tax_amount DECIMAL(10,2),
    shipping_cost DECIMAL(8,2),
    discount_amount DECIMAL(8,2),
    notes TEXT,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- INDEXES FOR PERFORMANCE
-- =====================================================

-- Source system indexes
CREATE INDEX IF NOT EXISTS idx_customers_email ON source_ecommerce.customers(email);
CREATE INDEX IF NOT EXISTS idx_customers_registration_date ON source_ecommerce.customers(registration_date);
CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON source_ecommerce.orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_order_date ON source_ecommerce.orders(order_date);
CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON source_ecommerce.order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON source_ecommerce.order_items(product_id);

-- Data warehouse indexes
CREATE INDEX IF NOT EXISTS idx_dim_customers_customer_id ON dw_analytics.dim_customers(customer_id);
CREATE INDEX IF NOT EXISTS idx_dim_products_product_id ON dw_analytics.dim_products(product_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_order_date_key ON dw_analytics.fact_sales(order_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer_key ON dw_analytics.fact_sales(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product_key ON dw_analytics.fact_sales(product_key);

