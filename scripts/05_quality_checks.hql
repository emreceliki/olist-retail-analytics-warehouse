SELECT '=== NULL CHECKS ===' AS check_type;

-- Check null order_ids in bronze
SELECT 
    'bronze.orders_raw - null order_id' AS check_name,
    COUNT(*) AS null_count,
    CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END AS status
FROM bronze.orders_raw
WHERE order_id IS NULL OR order_id = '';

-- Check null customer_ids in bronze
SELECT 
    'bronze.customers_raw - null customer_id' AS check_name,
    COUNT(*) AS null_count,
    CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END AS status
FROM bronze.customers_raw
WHERE customer_id IS NULL OR customer_id = '';

-- Check null product_ids in order items
SELECT 
    'bronze.order_items_raw - null product_id' AS check_name,
    COUNT(*) AS null_count,
    CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END AS status
FROM bronze.order_items_raw
WHERE product_id IS NULL OR product_id = '';

-- Check nulls in silver layer
SELECT 
    'silver.orders - null order_id' AS check_name,
    COUNT(*) AS null_count,
    CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END AS status
FROM silver.orders
WHERE order_id IS NULL;




SELECT '=== DUPLICATE CHECKS ===' AS check_type;

-- Duplicate order_ids in silver.orders
SELECT 
    'silver.orders - duplicate order_id' AS check_name,
    COUNT(*) AS duplicate_count,
    CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END AS status
FROM (
    SELECT order_id, COUNT(*) AS cnt
    FROM silver.orders
    GROUP BY order_id
    HAVING COUNT(*) > 1
) dups;

-- Duplicate customer_ids in silver.customers
SELECT 
    'silver.customers - duplicate customer_id' AS check_name,
    COUNT(*) AS duplicate_count,
    CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END AS status
FROM (
    SELECT customer_id, COUNT(*) AS cnt
    FROM silver.customers
    GROUP BY customer_id
    HAVING COUNT(*) > 1
) dups;

-- Duplicate product_ids in silver.products
SELECT 
    'silver.products - duplicate product_id' AS check_name,
    COUNT(*) AS duplicate_count,
    CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END AS status
FROM (
    SELECT product_id, COUNT(*) AS cnt
    FROM silver.products
    GROUP BY product_id
    HAVING COUNT(*) > 1
) dups;




SELECT '=== DATE VALIDATION ===' AS check_type;

-- Orders with future dates
SELECT 
    'silver.orders - future order dates' AS check_name,
    COUNT(*) AS invalid_count,
    CASE WHEN COUNT(*) > 0 THEN 'WARN' ELSE 'PASS' END AS status
FROM silver.orders
WHERE order_purchase_timestamp > CURRENT_TIMESTAMP();

-- Orders with delivery before purchase
SELECT 
    'silver.orders - delivery before purchase' AS check_name,
    COUNT(*) AS invalid_count,
    CASE WHEN COUNT(*) > 0 THEN 'WARN' ELSE 'PASS' END AS status
FROM silver.orders
WHERE order_delivered_customer_date < order_purchase_timestamp
  AND order_delivered_customer_date IS NOT NULL;

-- Check date partitions are valid
SELECT 
    'silver.orders - invalid dt partitions' AS check_name,
    COUNT(*) AS invalid_count,
    CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END AS status
FROM silver.orders
WHERE dt NOT RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$';




SELECT '=== AMOUNT VALIDATION ===' AS check_type;

-- Negative prices in order items
SELECT 
    'silver.order_items - negative prices' AS check_name,
    COUNT(*) AS invalid_count,
    CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END AS status
FROM silver.order_items
WHERE price < 0;

-- Negative freight values
SELECT 
    'silver.order_items - negative freight' AS check_name,
    COUNT(*) AS invalid_count,
    CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END AS status
FROM silver.order_items
WHERE freight_value < 0;

-- Zero or negative payment values
SELECT 
    'silver.payments - non-positive payment' AS check_name,
    COUNT(*) AS invalid_count,
    CASE WHEN COUNT(*) > 0 THEN 'WARN' ELSE 'PASS' END AS status
FROM silver.payments
WHERE total_payment_value <= 0;




SELECT '=== REFERENTIAL INTEGRITY ===' AS check_type;

-- Orders with missing customers
SELECT 
    'silver.orders - orphan customer_id' AS check_name,
    COUNT(*) AS orphan_count,
    CASE WHEN COUNT(*) > 0 THEN 'WARN' ELSE 'PASS' END AS status
FROM silver.orders o
LEFT JOIN silver.customers c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- Order items with missing orders
SELECT 
    'silver.order_items - orphan order_id' AS check_name,
    COUNT(*) AS orphan_count,
    CASE WHEN COUNT(*) > 0 THEN 'WARN' ELSE 'PASS' END AS status
FROM silver.order_items oi
LEFT JOIN silver.orders o ON oi.order_id = o.order_id
WHERE o.order_id IS NULL;

-- Order items with missing products
SELECT 
    'silver.order_items - orphan product_id' AS check_name,
    COUNT(*) AS orphan_count,
    CASE WHEN COUNT(*) > 0 THEN 'WARN' ELSE 'PASS' END AS status
FROM silver.order_items oi
LEFT JOIN silver.products p ON oi.product_id = p.product_id
WHERE p.product_id IS NULL;

-- Payments with missing orders
SELECT 
    'silver.payments - orphan order_id' AS check_name,
    COUNT(*) AS orphan_count,
    CASE WHEN COUNT(*) > 0 THEN 'WARN' ELSE 'PASS' END AS status
FROM silver.payments py
LEFT JOIN silver.orders o ON py.order_id = o.order_id
WHERE o.order_id IS NULL;




SELECT '=== GOLD LAYER VALIDATION ===' AS check_type;

-- Check RFM features completeness
SELECT 
    'gold.rfm_features - missing segments' AS check_name,
    COUNT(*) AS null_count,
    CASE WHEN COUNT(*) > 0 THEN 'WARN' ELSE 'PASS' END AS status
FROM gold.rfm_features
WHERE rfm_segment IS NULL OR rfm_segment = '';

-- Check churn labels completeness
SELECT 
    'gold.customer_churn_labels - invalid is_churned' AS check_name,
    COUNT(*) AS invalid_count,
    CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END AS status
FROM gold.customer_churn_labels
WHERE is_churned NOT IN (0, 1);

-- Check daily KPI has no negative values
SELECT 
    'gold.daily_kpi - negative GMV' AS check_name,
    COUNT(*) AS invalid_count,
    CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END AS status
FROM gold.daily_kpi
WHERE gmv < 0;




SELECT '=== DATA COMPLETENESS SUMMARY ===' AS check_type;

SELECT
    'bronze' AS layer,
    'orders' AS table_name,
    COUNT(*) AS row_count
FROM bronze.orders_raw
UNION ALL
SELECT 'bronze', 'customers', COUNT(*) FROM bronze.customers_raw
UNION ALL
SELECT 'bronze', 'order_items', COUNT(*) FROM bronze.order_items_raw
UNION ALL
SELECT 'bronze', 'products', COUNT(*) FROM bronze.products_raw
UNION ALL
SELECT 'bronze', 'payments', COUNT(*) FROM bronze.payments_raw
UNION ALL
SELECT 'silver', 'orders', COUNT(*) FROM silver.orders
UNION ALL
SELECT 'silver', 'customers', COUNT(*) FROM silver.customers
UNION ALL
SELECT 'silver', 'order_items', COUNT(*) FROM silver.order_items
UNION ALL
SELECT 'silver', 'products', COUNT(*) FROM silver.products
UNION ALL
SELECT 'silver', 'payments', COUNT(*) FROM silver.payments
UNION ALL
SELECT 'gold', 'daily_kpi', COUNT(*) FROM gold.daily_kpi
UNION ALL
SELECT 'gold', 'cohort_retention', COUNT(*) FROM gold.cohort_retention
UNION ALL
SELECT 'gold', 'rfm_features', COUNT(*) FROM gold.rfm_features
UNION ALL
SELECT 'gold', 'customer_churn_labels', COUNT(*) FROM gold.customer_churn_labels;




SELECT '=== DATE RANGE SUMMARY ===' AS check_type;

SELECT
    MIN(dt) AS min_date,
    MAX(dt) AS max_date,
    COUNT(DISTINCT dt) AS distinct_days
FROM silver.orders;
