-- ********************************************
-- Restaurant Data Pipeline - Database Script
-- ********************************************

-- This script creates all necessary database tables for the 
-- restaurant data pipeline, including staging and target tables.

-- Database schema creation and table definitions

-- ********************************
-- Staging Tables
-- ********************************

-- Staging table for orders
CREATE TABLE IF NOT EXISTS staging_orders (
    order_id INTEGER PRIMARY KEY,
    customer_id VARCHAR(50),
    order_date VARCHAR(50),
    total_amount FLOAT
);

-- Staging table for order items
CREATE TABLE IF NOT EXISTS staging_order_items (
    id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL,
    item_id VARCHAR(50) NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price FLOAT NOT NULL
);

-- Staging table for menu items
CREATE TABLE IF NOT EXISTS staging_menu_items (
    item_id VARCHAR(50) PRIMARY KEY,
    item_name VARCHAR(100) NOT NULL,
    category VARCHAR(50) NOT NULL,
    description VARCHAR(500)
);

-- ********************************
-- Target Tables
-- ********************************

-- Complete orders table (joined data)
CREATE TABLE IF NOT EXISTS complete_orders (
    id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL,
    customer_id VARCHAR(50),
    order_date DATE NOT NULL,
    item_id VARCHAR(50) NOT NULL,
    item_name VARCHAR(100) NOT NULL,
    category VARCHAR(50) NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price FLOAT NOT NULL,
    item_revenue FLOAT NOT NULL
);

-- Daily revenue by category
CREATE TABLE IF NOT EXISTS daily_revenue_by_category (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    category VARCHAR(50) NOT NULL,
    total_revenue FLOAT NOT NULL,
    order_count INTEGER NOT NULL,
    CONSTRAINT unique_date_category UNIQUE (date, category)
);

-- Top selling items
CREATE TABLE IF NOT EXISTS top_selling_items (
    id SERIAL PRIMARY KEY,
    item_id VARCHAR(50) NOT NULL,
    item_name VARCHAR(100) NOT NULL,
    category VARCHAR(50) NOT NULL,
    total_quantity_sold INTEGER NOT NULL,
    total_revenue FLOAT NOT NULL,
    CONSTRAINT unique_item_id UNIQUE (item_id)
);

-- Category metrics
CREATE TABLE IF NOT EXISTS category_metrics (
    id SERIAL PRIMARY KEY,
    category VARCHAR(50) NOT NULL,
    order_count INTEGER NOT NULL,
    total_revenue FLOAT NOT NULL,
    total_items_sold INTEGER NOT NULL,
    unique_items_count INTEGER NOT NULL,
    avg_revenue_per_order FLOAT NOT NULL,
    avg_items_per_order FLOAT NOT NULL,
    CONSTRAINT unique_category UNIQUE (category)
);

-- ********************************
-- Indexes
-- ********************************

-- Indexes for staging tables
CREATE INDEX IF NOT EXISTS idx_staging_orders_date ON staging_orders(order_date);
CREATE INDEX IF NOT EXISTS idx_staging_order_items_order_id ON staging_order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_staging_order_items_item_id ON staging_order_items(item_id);
CREATE INDEX IF NOT EXISTS idx_staging_menu_items_category ON staging_menu_items(category);

-- Indexes for complete orders
CREATE INDEX IF NOT EXISTS idx_complete_orders_order_id ON complete_orders(order_id);
CREATE INDEX IF NOT EXISTS idx_complete_orders_order_date ON complete_orders(order_date);
CREATE INDEX IF NOT EXISTS idx_complete_orders_category ON complete_orders(category);
CREATE INDEX IF NOT EXISTS idx_complete_orders_item_id ON complete_orders(item_id);

-- Indexes for daily revenue
CREATE INDEX IF NOT EXISTS idx_daily_revenue_date ON daily_revenue_by_category(date);
CREATE INDEX IF NOT EXISTS idx_daily_revenue_category ON daily_revenue_by_category(category);

-- Indexes for top selling items
CREATE INDEX IF NOT EXISTS idx_top_selling_category ON top_selling_items(category);
CREATE INDEX IF NOT EXISTS idx_top_selling_total_quantity ON top_selling_items(total_quantity_sold DESC);
CREATE INDEX IF NOT EXISTS idx_top_selling_total_revenue ON top_selling_items(total_revenue DESC);

-- Indexes for category metrics
CREATE INDEX IF NOT EXISTS idx_category_metrics_total_revenue ON category_metrics(total_revenue DESC);

