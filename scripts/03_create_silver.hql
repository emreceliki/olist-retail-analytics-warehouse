SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions = 10000;
SET hive.exec.max.dynamic.partitions.pernode = 1000;

-- Create Silver database
CREATE DATABASE IF NOT EXISTS silver
COMMENT 'Curated data layer - Cleaned and transformed tables'
LOCATION '/user/hive/warehouse/silver.db';

USE silver;


DROP TABLE IF EXISTS customers;

CREATE TABLE customers (
    customer_id              STRING COMMENT 'Order-level customer ID',
    customer_unique_id       STRING COMMENT 'Unique customer identifier across orders',
    customer_zip_code_prefix INT    COMMENT 'Customer ZIP code prefix',
    customer_city            STRING COMMENT 'Customer city name',
    customer_state           STRING COMMENT 'Customer state code (2 letters)'
)
STORED AS ORC
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'transactional' = 'false'
);

INSERT OVERWRITE TABLE customers
SELECT
    customer_id,
    customer_unique_id,
    CAST(customer_zip_code_prefix AS INT) AS customer_zip_code_prefix,
    LOWER(TRIM(customer_city)) AS customer_city,
    UPPER(TRIM(customer_state)) AS customer_state
FROM (
    SELECT
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY customer_id) AS rn
    FROM bronze.customers_raw
    WHERE customer_id IS NOT NULL
      AND customer_id != ''
) deduped
WHERE rn = 1;




DROP TABLE IF EXISTS orders;

CREATE TABLE orders (
    order_id                        STRING    COMMENT 'Unique order identifier',
    customer_id                     STRING    COMMENT 'Customer ID (FK to customers)',
    order_status                    STRING    COMMENT 'Order status (delivered, shipped, etc.)',
    order_purchase_timestamp        TIMESTAMP COMMENT 'Order purchase timestamp',
    order_approved_at               TIMESTAMP COMMENT 'Payment approval timestamp',
    order_delivered_carrier_date    TIMESTAMP COMMENT 'Carrier pickup timestamp',
    order_delivered_customer_date   TIMESTAMP COMMENT 'Customer delivery timestamp',
    order_estimated_delivery_date   DATE      COMMENT 'Estimated delivery date'
)
PARTITIONED BY (dt STRING COMMENT 'Partition by purchase date (YYYY-MM-DD)')
STORED AS ORC
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'transactional' = 'false'
);

INSERT OVERWRITE TABLE orders PARTITION (dt)
SELECT
    order_id,
    customer_id,
    LOWER(TRIM(order_status)) AS order_status,
    CAST(order_purchase_timestamp AS TIMESTAMP) AS order_purchase_timestamp,
    CAST(NULLIF(order_approved_at, '') AS TIMESTAMP) AS order_approved_at,
    CAST(NULLIF(order_delivered_carrier_date, '') AS TIMESTAMP) AS order_delivered_carrier_date,
    CAST(NULLIF(order_delivered_customer_date, '') AS TIMESTAMP) AS order_delivered_customer_date,
    CAST(NULLIF(order_estimated_delivery_date, '') AS DATE) AS order_estimated_delivery_date,
    SUBSTR(order_purchase_timestamp, 1, 10) AS dt
FROM (
    SELECT
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_purchase_timestamp) AS rn
    FROM bronze.orders_raw
    WHERE order_id IS NOT NULL
      AND order_id != ''
      AND order_purchase_timestamp IS NOT NULL
      AND order_purchase_timestamp != ''
) deduped
WHERE rn = 1;




DROP TABLE IF EXISTS order_items;

CREATE TABLE order_items (
    order_id            STRING  COMMENT 'Order ID (FK to orders)',
    order_item_id       INT     COMMENT 'Item sequence within order',
    product_id          STRING  COMMENT 'Product ID (FK to products)',
    seller_id           STRING  COMMENT 'Seller ID',
    shipping_limit_date TIMESTAMP COMMENT 'Seller shipping deadline',
    price               DOUBLE  COMMENT 'Item price (BRL)',
    freight_value       DOUBLE  COMMENT 'Freight cost (BRL)',
    total_value         DOUBLE  COMMENT 'Total item value (price + freight)'
)
STORED AS ORC
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'transactional' = 'false'
);

INSERT OVERWRITE TABLE order_items
SELECT
    order_id,
    CAST(order_item_id AS INT) AS order_item_id,
    product_id,
    seller_id,
    CAST(NULLIF(shipping_limit_date, '') AS TIMESTAMP) AS shipping_limit_date,
    CAST(price AS DOUBLE) AS price,
    CAST(freight_value AS DOUBLE) AS freight_value,
    CAST(price AS DOUBLE) + CAST(freight_value AS DOUBLE) AS total_value
FROM bronze.order_items_raw
WHERE order_id IS NOT NULL
  AND order_id != ''
  AND product_id IS NOT NULL
  AND product_id != '';




DROP TABLE IF EXISTS products;

CREATE TABLE products (
    product_id                 STRING COMMENT 'Unique product identifier',
    product_category_name      STRING COMMENT 'Category name (Portuguese)',
    product_category_english   STRING COMMENT 'Category name (English)',
    product_name_length        INT    COMMENT 'Product name character length',
    product_description_length INT    COMMENT 'Description character length',
    product_photos_qty         INT    COMMENT 'Number of product photos',
    product_weight_g           INT    COMMENT 'Product weight in grams',
    product_length_cm          INT    COMMENT 'Product length in cm',
    product_height_cm          INT    COMMENT 'Product height in cm',
    product_width_cm           INT    COMMENT 'Product width in cm'
)
STORED AS ORC
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'transactional' = 'false'
);

INSERT OVERWRITE TABLE products
SELECT
    p.product_id,
    LOWER(TRIM(p.product_category_name)) AS product_category_name,
    COALESCE(LOWER(TRIM(t.product_category_name_english)), 'unknown') AS product_category_english,
    CAST(NULLIF(p.product_name_lenght, '') AS INT) AS product_name_length,
    CAST(NULLIF(p.product_description_lenght, '') AS INT) AS product_description_length,
    CAST(NULLIF(p.product_photos_qty, '') AS INT) AS product_photos_qty,
    CAST(NULLIF(p.product_weight_g, '') AS INT) AS product_weight_g,
    CAST(NULLIF(p.product_length_cm, '') AS INT) AS product_length_cm,
    CAST(NULLIF(p.product_height_cm, '') AS INT) AS product_height_cm,
    CAST(NULLIF(p.product_width_cm, '') AS INT) AS product_width_cm
FROM (
    SELECT
        product_id,
        product_category_name,
        product_name_lenght,
        product_description_lenght,
        product_photos_qty,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_id) AS rn
    FROM bronze.products_raw
    WHERE product_id IS NOT NULL
      AND product_id != ''
) p
LEFT JOIN bronze.category_translation_raw t
    ON LOWER(TRIM(p.product_category_name)) = LOWER(TRIM(t.product_category_name))
WHERE p.rn = 1;




DROP TABLE IF EXISTS payments;

CREATE TABLE payments (
    order_id                STRING COMMENT 'Order ID (FK to orders)',
    total_payment_value     DOUBLE COMMENT 'Total payment amount for order',
    payment_count           INT    COMMENT 'Number of payment transactions',
    primary_payment_type    STRING COMMENT 'Primary payment method used',
    avg_installments        DOUBLE COMMENT 'Average installments across payments'
)
STORED AS ORC
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'transactional' = 'false'
);

INSERT OVERWRITE TABLE payments
SELECT
    order_id,
    SUM(CAST(payment_value AS DOUBLE)) AS total_payment_value,
    COUNT(*) AS payment_count,
    -- Get the primary payment type (most used or highest value)
    MAX(payment_type) AS primary_payment_type,
    AVG(CAST(payment_installments AS DOUBLE)) AS avg_installments
FROM bronze.payments_raw
WHERE order_id IS NOT NULL
  AND order_id != ''
GROUP BY order_id;




ANALYZE TABLE customers COMPUTE STATISTICS;
ANALYZE TABLE orders COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE order_items COMPUTE STATISTICS;
ANALYZE TABLE products COMPUTE STATISTICS;
ANALYZE TABLE payments COMPUTE STATISTICS;




SHOW TABLES IN silver;

-- Sample row counts
SELECT 'customers' as table_name, COUNT(*) as row_count FROM customers
UNION ALL
SELECT 'orders', COUNT(*) FROM orders
UNION ALL
SELECT 'order_items', COUNT(*) FROM order_items
UNION ALL
SELECT 'products', COUNT(*) FROM products
UNION ALL
SELECT 'payments', COUNT(*) FROM payments;
