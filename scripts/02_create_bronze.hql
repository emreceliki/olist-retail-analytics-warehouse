-- Create Bronze database
CREATE DATABASE IF NOT EXISTS bronze
COMMENT 'Raw data layer - External tables pointing to HDFS CSV files'
LOCATION '/user/hive/warehouse/bronze.db';

USE bronze;


DROP TABLE IF EXISTS orders;

CREATE EXTERNAL TABLE orders (
    order_id                        STRING,
    customer_id                     STRING,
    order_status                    STRING,
    order_purchase_timestamp        STRING,
    order_approved_at               STRING,
    order_delivered_carrier_date    STRING,
    order_delivered_customer_date   STRING,
    order_estimated_delivery_date   STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar' = '"',
    'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION '/data/bronze/olist/'
TBLPROPERTIES (
    'skip.header.line.count' = '1',
    'serialization.null.format' = ''
);

-- Note: We'll create a view or use a specific file pattern
-- For now, we create separate tables for each file

DROP TABLE IF EXISTS orders_raw;

CREATE EXTERNAL TABLE orders_raw (
    order_id                        STRING,
    customer_id                     STRING,
    order_status                    STRING,
    order_purchase_timestamp        STRING,
    order_approved_at               STRING,
    order_delivered_carrier_date    STRING,
    order_delivered_customer_date   STRING,
    order_estimated_delivery_date   STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar' = '"',
    'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION '/data/bronze/olist/orders/'
TBLPROPERTIES (
    'skip.header.line.count' = '1',
    'serialization.null.format' = ''
);




DROP TABLE IF EXISTS customers_raw;

CREATE EXTERNAL TABLE customers_raw (
    customer_id              STRING,
    customer_unique_id       STRING,
    customer_zip_code_prefix STRING,
    customer_city            STRING,
    customer_state           STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar' = '"',
    'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION '/data/bronze/olist/customers/'
TBLPROPERTIES (
    'skip.header.line.count' = '1',
    'serialization.null.format' = ''
);




DROP TABLE IF EXISTS order_items_raw;

CREATE EXTERNAL TABLE order_items_raw (
    order_id            STRING,
    order_item_id       STRING,
    product_id          STRING,
    seller_id           STRING,
    shipping_limit_date STRING,
    price               STRING,
    freight_value       STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar' = '"',
    'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION '/data/bronze/olist/order_items/'
TBLPROPERTIES (
    'skip.header.line.count' = '1',
    'serialization.null.format' = ''
);




DROP TABLE IF EXISTS products_raw;

CREATE EXTERNAL TABLE products_raw (
    product_id                 STRING,
    product_category_name      STRING,
    product_name_lenght        STRING,
    product_description_lenght STRING,
    product_photos_qty         STRING,
    product_weight_g           STRING,
    product_length_cm          STRING,
    product_height_cm          STRING,
    product_width_cm           STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar' = '"',
    'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION '/data/bronze/olist/products/'
TBLPROPERTIES (
    'skip.header.line.count' = '1',
    'serialization.null.format' = ''
);




DROP TABLE IF EXISTS payments_raw;

CREATE EXTERNAL TABLE payments_raw (
    order_id             STRING,
    payment_sequential   STRING,
    payment_type         STRING,
    payment_installments STRING,
    payment_value        STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar' = '"',
    'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION '/data/bronze/olist/payments/'
TBLPROPERTIES (
    'skip.header.line.count' = '1',
    'serialization.null.format' = ''
);




DROP TABLE IF EXISTS category_translation_raw;

CREATE EXTERNAL TABLE category_translation_raw (
    product_category_name         STRING,
    product_category_name_english STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar' = '"',
    'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION '/data/bronze/olist/category_translation/'
TBLPROPERTIES (
    'skip.header.line.count' = '1',
    'serialization.null.format' = ''
);




SHOW TABLES IN bronze;

-- Sample counts (run after data ingestion)
-- SELECT 'orders' as table_name, COUNT(*) as row_count FROM orders_raw
-- UNION ALL
-- SELECT 'customers', COUNT(*) FROM customers_raw
-- UNION ALL
-- SELECT 'order_items', COUNT(*) FROM order_items_raw
-- UNION ALL
-- SELECT 'products', COUNT(*) FROM products_raw
-- UNION ALL
-- SELECT 'payments', COUNT(*) FROM payments_raw
-- UNION ALL
-- SELECT 'category_translation', COUNT(*) FROM category_translation_raw;
