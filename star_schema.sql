-- Dimension: Date
CREATE TABLE dim_date (
    date_id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20),
    day INT NOT NULL,
    day_name VARCHAR(20),
    week_day INT NOT NULL
);

-- Dimension: Season
CREATE TABLE dim_season (
    season_id SERIAL PRIMARY KEY,
    season VARCHAR(20) NOT NULL
);

-- Dimension: Customer
CREATE TABLE dim_customer (
    customer_id SERIAL PRIMARY KEY,
    customer_name VARCHAR(255) NOT NULL,
    customer_category VARCHAR(50)
);

-- Dimension: Store
CREATE TABLE dim_store (
    store_id SERIAL PRIMARY KEY,
    store_type VARCHAR(50) NOT NULL,
    city VARCHAR(100) NOT NULL
);

-- Dimension: Payment Method
CREATE TABLE dim_payment_method (
    payment_method_id SERIAL PRIMARY KEY,
    payment_method VARCHAR(50) NOT NULL
);

-- Dimension: Product
CREATE TABLE dim_product (
    product_id SERIAL PRIMARY KEY,
    product VARCHAR(255) NOT NULL
);

-- Dimension: Promotion
CREATE TABLE dim_promotion (
    promotion_id SERIAL PRIMARY KEY,
    promotion VARCHAR(255) NOT NULL
);

-- Fact Table: Sales
CREATE TABLE fact_sales (
    transaction_id BIGINT PRIMARY KEY,
    store_id INT REFERENCES dim_store(store_id),
    customer_id INT REFERENCES dim_customer(customer_id),
    product_id INT REFERENCES dim_product(product_id),
    promotion_id INT REFERENCES dim_promotion(promotion_id),
    payment_method_id INT REFERENCES dim_payment_method(payment_method_id),
    season_id INT REFERENCES dim_season(season_id),
    date_id INT REFERENCES dim_date(date_id),
    total_items INT NOT NULL,
    total_cost NUMERIC(10, 2) NOT NULL,
    discount_applied BOOLEAN NOT NULL
);


git remote set-url origin https://soyglorialopez@github.com/<repository-owner>/etl.git
git remote set-url origin https://github.com/soyglorialopez/etl.git
