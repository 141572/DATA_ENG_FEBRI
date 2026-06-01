-- Schema Initialization for Olist Brazilian E-Commerce Data Warehouse (Star Schema)

-- 1. Create Dimension Tables

-- Dimension: Customers
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_unique_id VARCHAR(50) NOT NULL,
    customer_zip_code_prefix INT,
    customer_city VARCHAR(100),
    customer_state VARCHAR(10)
);

-- Dimension: Products
CREATE TABLE IF NOT EXISTS dim_products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_category_name VARCHAR(100),
    product_category_name_english VARCHAR(100),
    product_name_length INT,
    product_description_length INT,
    product_photos_qty INT,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT
);

-- Dimension: Sellers
CREATE TABLE IF NOT EXISTS dim_sellers (
    seller_id VARCHAR(50) PRIMARY KEY,
    seller_zip_code_prefix INT,
    seller_city VARCHAR(100),
    seller_state VARCHAR(10)
);

-- Dimension: Time (for advanced time-based querying)
CREATE TABLE IF NOT EXISTS dim_time (
    time_key TIMESTAMP PRIMARY KEY,
    date DATE NOT NULL,
    day INT NOT NULL,
    week INT NOT NULL,
    month INT NOT NULL,
    quarter INT NOT NULL,
    year INT NOT NULL,
    day_of_week INT NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

-- Dimension: Reviews
CREATE TABLE IF NOT EXISTS dim_reviews (
    review_id VARCHAR(50) PRIMARY KEY,
    review_score INT CHECK (review_score BETWEEN 1 AND 5),
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date TIMESTAMP,
    review_answer_timestamp TIMESTAMP
);

-- 2. Create Fact Table

-- Fact: Sales Transactions (Granularity: Order Item Level)
CREATE TABLE IF NOT EXISTS fact_sales_transactions (
    transaction_id SERIAL PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL,
    order_item_id INT NOT NULL,
    customer_id VARCHAR(50) REFERENCES dim_customers(customer_id),
    product_id VARCHAR(50) REFERENCES dim_products(product_id),
    seller_id VARCHAR(50) REFERENCES dim_sellers(seller_id),
    review_id VARCHAR(50) REFERENCES dim_reviews(review_id),
    purchase_timestamp TIMESTAMP REFERENCES dim_time(time_key),
    approved_timestamp TIMESTAMP,
    delivered_carrier_date TIMESTAMP,
    delivered_customer_date TIMESTAMP,
    estimated_delivery_date TIMESTAMP,
    price DECIMAL(10, 2) NOT NULL,
    freight_value DECIMAL(10, 2) NOT NULL,
    payment_sequential INT,
    payment_type VARCHAR(50),
    payment_installments INT,
    payment_value DECIMAL(10, 2),
    order_status VARCHAR(50),
    shipping_duration_days INT,
    delivery_delay_days INT
);

-- 3. Create Indexes for Performance Optimization

CREATE INDEX IF NOT EXISTS idx_fact_customer ON fact_sales_transactions(customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_product ON fact_sales_transactions(product_id);
CREATE INDEX IF NOT EXISTS idx_fact_seller ON fact_sales_transactions(seller_id);
CREATE INDEX IF NOT EXISTS idx_fact_review ON fact_sales_transactions(review_id);
CREATE INDEX IF NOT EXISTS idx_fact_time ON fact_sales_transactions(purchase_timestamp);
CREATE INDEX IF NOT EXISTS idx_dim_cust_unique ON dim_customers(customer_unique_id);
CREATE INDEX IF NOT EXISTS idx_dim_prod_cat ON dim_products(product_category_name_english);

-- 4. Create Analytical Views for BI & Reporting

-- View 1: Core Financial & KPI Metrics (Revenue, Order Counts, AOV)
CREATE OR REPLACE VIEW view_kpi_financial_metrics AS
SELECT
    t.year,
    t.month,
    t.date,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.price) AS net_product_revenue,
    SUM(f.freight_value) AS total_shipping_costs,
    SUM(f.payment_value) AS gross_payment_revenue,
    AVG(f.price) AS average_item_price,
    SUM(f.price) / NULLIF(COUNT(DISTINCT f.order_id), 0) AS average_order_value_aov
FROM fact_sales_transactions f
JOIN dim_time t ON f.purchase_timestamp = t.time_key
WHERE f.order_status = 'delivered'
GROUP BY t.year, t.month, t.date;

-- View 2: Geospatial Sales & Customer Distribution
CREATE OR REPLACE VIEW view_geospatial_sales_distribution AS
SELECT
    c.customer_state AS state,
    c.customer_city AS city,
    COUNT(DISTINCT f.order_id) AS order_count,
    SUM(f.price) AS total_sales,
    AVG(f.freight_value) AS avg_freight
FROM fact_sales_transactions f
JOIN dim_customers c ON f.customer_id = c.customer_id
WHERE f.order_status = 'delivered'
GROUP BY c.customer_state, c.customer_city;

-- View 3: Seller Performance (Revenue, Shipping Speed, Ratings)
CREATE OR REPLACE VIEW view_seller_performance AS
SELECT
    s.seller_id,
    s.seller_city,
    s.seller_state,
    COUNT(DISTINCT f.order_id) AS total_sales_orders,
    SUM(f.price) AS total_seller_revenue,
    AVG(f.shipping_duration_days) AS avg_shipping_duration_days,
    AVG(f.delivery_delay_days) AS avg_delivery_delay_days,
    AVG(r.review_score) AS average_seller_rating
FROM fact_sales_transactions f
JOIN dim_sellers s ON f.seller_id = s.seller_id
LEFT JOIN dim_reviews r ON f.review_id = r.review_id
WHERE f.order_status = 'delivered'
GROUP BY s.seller_id, s.seller_city, s.seller_state;

-- View 4: Product Category Insights & Sentiment Analysis
CREATE OR REPLACE VIEW view_product_category_insights AS
SELECT
    p.product_category_name_english AS product_category,
    COUNT(f.transaction_id) AS items_sold_count,
    SUM(f.price) AS total_sales_value,
    AVG(r.review_score) AS average_review_score,
    COUNT(CASE WHEN r.review_score = 5 THEN 1 END) * 100.0 / NULLIF(COUNT(r.review_id), 0) AS five_star_rating_percentage
FROM fact_sales_transactions f
JOIN dim_products p ON f.product_id = p.product_id
LEFT JOIN dim_reviews r ON f.review_id = r.review_id
GROUP BY p.product_category_name_english;
