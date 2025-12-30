SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

-- Create Gold database
CREATE DATABASE IF NOT EXISTS gold
COMMENT 'Analytics layer - Business metrics and ML features'
LOCATION '/user/hive/warehouse/gold.db';

USE gold;


DROP TABLE IF EXISTS daily_kpi;

CREATE TABLE daily_kpi (
    dt                   STRING COMMENT 'Date (YYYY-MM-DD)',
    order_count          BIGINT COMMENT 'Total orders placed',
    gmv                  DOUBLE COMMENT 'Gross Merchandise Value (BRL)',
    avg_order_value      DOUBLE COMMENT 'Average Order Value (BRL)',
    unique_customers     BIGINT COMMENT 'Unique customers who ordered',
    total_items          BIGINT COMMENT 'Total items sold',
    avg_items_per_order  DOUBLE COMMENT 'Average items per order',
    total_freight        DOUBLE COMMENT 'Total freight revenue (BRL)'
)
STORED AS ORC
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY'
);

INSERT OVERWRITE TABLE daily_kpi
SELECT
    o.dt,
    COUNT(DISTINCT o.order_id) AS order_count,
    SUM(oi.price) AS gmv,
    SUM(oi.price) / COUNT(DISTINCT o.order_id) AS avg_order_value,
    COUNT(DISTINCT o.customer_id) AS unique_customers,
    COUNT(oi.order_item_id) AS total_items,
    COUNT(oi.order_item_id) / COUNT(DISTINCT o.order_id) AS avg_items_per_order,
    SUM(oi.freight_value) AS total_freight
FROM silver.orders o
JOIN silver.order_items oi ON o.order_id = oi.order_id
WHERE o.order_status = 'delivered'
GROUP BY o.dt
ORDER BY o.dt;




DROP TABLE IF EXISTS daily_kpi_by_category;

CREATE TABLE daily_kpi_by_category (
    dt                   STRING COMMENT 'Date (YYYY-MM-DD)',
    category             STRING COMMENT 'Product category (English)',
    order_count          BIGINT COMMENT 'Orders containing this category',
    gmv                  DOUBLE COMMENT 'Category GMV (BRL)',
    item_count           BIGINT COMMENT 'Items sold in category'
)
STORED AS ORC
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY'
);

INSERT OVERWRITE TABLE daily_kpi_by_category
SELECT
    o.dt,
    COALESCE(p.product_category_english, 'unknown') AS category,
    COUNT(DISTINCT o.order_id) AS order_count,
    SUM(oi.price) AS gmv,
    COUNT(oi.order_item_id) AS item_count
FROM silver.orders o
JOIN silver.order_items oi ON o.order_id = oi.order_id
LEFT JOIN silver.products p ON oi.product_id = p.product_id
WHERE o.order_status = 'delivered'
GROUP BY o.dt, COALESCE(p.product_category_english, 'unknown')
ORDER BY o.dt, gmv DESC;




DROP TABLE IF EXISTS cohort_retention;

CREATE TABLE cohort_retention (
    cohort_month         STRING COMMENT 'Cohort month (first purchase YYYY-MM)',
    activity_month       STRING COMMENT 'Activity month (YYYY-MM)',
    months_since_cohort  INT    COMMENT 'Months since first purchase (M0, M1, ...)',
    cohort_size          BIGINT COMMENT 'Total customers in cohort',
    active_customers     BIGINT COMMENT 'Customers active in activity month',
    retention_rate       DOUBLE COMMENT 'Retention rate (0-1)'
)
STORED AS ORC
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY'
);

-- First, create customer cohorts
DROP TABLE IF EXISTS temp_customer_cohorts;
CREATE TEMPORARY TABLE temp_customer_cohorts AS
SELECT
    c.customer_unique_id,
    MIN(SUBSTR(o.dt, 1, 7)) AS cohort_month
FROM silver.orders o
JOIN silver.customers c ON o.customer_id = c.customer_id
WHERE o.order_status = 'delivered'
GROUP BY c.customer_unique_id;

-- Then, get monthly activity
DROP TABLE IF EXISTS temp_monthly_activity;
CREATE TEMPORARY TABLE temp_monthly_activity AS
SELECT DISTINCT
    c.customer_unique_id,
    SUBSTR(o.dt, 1, 7) AS activity_month
FROM silver.orders o
JOIN silver.customers c ON o.customer_id = c.customer_id
WHERE o.order_status = 'delivered';

-- Calculate cohort retention
INSERT OVERWRITE TABLE cohort_retention
SELECT
    coh.cohort_month,
    act.activity_month,
    -- Calculate months difference
    (YEAR(CONCAT(act.activity_month, '-01')) - YEAR(CONCAT(coh.cohort_month, '-01'))) * 12 +
    (MONTH(CONCAT(act.activity_month, '-01')) - MONTH(CONCAT(coh.cohort_month, '-01'))) AS months_since_cohort,
    cohort_counts.cohort_size,
    COUNT(DISTINCT act.customer_unique_id) AS active_customers,
    CAST(COUNT(DISTINCT act.customer_unique_id) AS DOUBLE) / cohort_counts.cohort_size AS retention_rate
FROM temp_customer_cohorts coh
JOIN temp_monthly_activity act ON coh.customer_unique_id = act.customer_unique_id
JOIN (
    SELECT cohort_month, COUNT(*) AS cohort_size
    FROM temp_customer_cohorts
    GROUP BY cohort_month
) cohort_counts ON coh.cohort_month = cohort_counts.cohort_month
WHERE act.activity_month >= coh.cohort_month
GROUP BY coh.cohort_month, act.activity_month, cohort_counts.cohort_size
ORDER BY coh.cohort_month, act.activity_month;




DROP TABLE IF EXISTS rfm_features;

CREATE TABLE rfm_features (
    customer_unique_id   STRING COMMENT 'Unique customer identifier',
    recency_days        INT    COMMENT 'Days since last purchase',
    frequency           INT    COMMENT 'Total number of orders',
    monetary            DOUBLE COMMENT 'Total spend (BRL)',
    first_order_date    DATE   COMMENT 'First order date',
    last_order_date     DATE   COMMENT 'Most recent order date',
    avg_order_value     DOUBLE COMMENT 'Average order value',
    r_score             INT    COMMENT 'Recency score (1-5, 5=most recent)',
    f_score             INT    COMMENT 'Frequency score (1-5, 5=most frequent)',
    m_score             INT    COMMENT 'Monetary score (1-5, 5=highest value)',
    rfm_segment         STRING COMMENT 'RFM segment label'
)
STORED AS ORC
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY'
);

-- Calculate base RFM metrics
DROP TABLE IF EXISTS temp_rfm_base;
CREATE TEMPORARY TABLE temp_rfm_base AS
SELECT
    c.customer_unique_id,
    DATEDIFF(
        (SELECT MAX(TO_DATE(dt)) FROM silver.orders),
        MAX(TO_DATE(o.dt))
    ) AS recency_days,
    COUNT(DISTINCT o.order_id) AS frequency,
    SUM(oi.price) AS monetary,
    MIN(TO_DATE(o.dt)) AS first_order_date,
    MAX(TO_DATE(o.dt)) AS last_order_date,
    SUM(oi.price) / COUNT(DISTINCT o.order_id) AS avg_order_value
FROM silver.orders o
JOIN silver.customers c ON o.customer_id = c.customer_id
JOIN silver.order_items oi ON o.order_id = oi.order_id
WHERE o.order_status = 'delivered'
GROUP BY c.customer_unique_id;

-- Calculate RFM scores using NTILE
INSERT OVERWRITE TABLE rfm_features
SELECT
    customer_unique_id,
    recency_days,
    frequency,
    monetary,
    first_order_date,
    last_order_date,
    avg_order_value,
    -- R score: lower recency = higher score
    NTILE(5) OVER (ORDER BY recency_days DESC) AS r_score,
    -- F score: higher frequency = higher score
    NTILE(5) OVER (ORDER BY frequency ASC) AS f_score,
    -- M score: higher monetary = higher score
    NTILE(5) OVER (ORDER BY monetary ASC) AS m_score,
    -- RFM Segment based on scores
    CASE
        WHEN NTILE(5) OVER (ORDER BY recency_days DESC) >= 4 
         AND NTILE(5) OVER (ORDER BY frequency ASC) >= 4 
         AND NTILE(5) OVER (ORDER BY monetary ASC) >= 4 THEN 'Champions'
        WHEN NTILE(5) OVER (ORDER BY recency_days DESC) >= 4 
         AND NTILE(5) OVER (ORDER BY frequency ASC) >= 2 THEN 'Loyal Customers'
        WHEN NTILE(5) OVER (ORDER BY recency_days DESC) >= 3 
         AND NTILE(5) OVER (ORDER BY frequency ASC) >= 3 THEN 'Potential Loyalists'
        WHEN NTILE(5) OVER (ORDER BY recency_days DESC) >= 4 
         AND NTILE(5) OVER (ORDER BY frequency ASC) = 1 THEN 'New Customers'
        WHEN NTILE(5) OVER (ORDER BY recency_days DESC) >= 3 
         AND NTILE(5) OVER (ORDER BY frequency ASC) <= 2 
         AND NTILE(5) OVER (ORDER BY monetary ASC) >= 3 THEN 'Promising'
        WHEN NTILE(5) OVER (ORDER BY recency_days DESC) = 2 
         AND NTILE(5) OVER (ORDER BY frequency ASC) >= 2 THEN 'Customers Needing Attention'
        WHEN NTILE(5) OVER (ORDER BY recency_days DESC) = 2 
         AND NTILE(5) OVER (ORDER BY frequency ASC) <= 2 THEN 'About to Sleep'
        WHEN NTILE(5) OVER (ORDER BY recency_days DESC) <= 2 
         AND NTILE(5) OVER (ORDER BY frequency ASC) >= 4 THEN 'At Risk'
        WHEN NTILE(5) OVER (ORDER BY recency_days DESC) = 1 
         AND NTILE(5) OVER (ORDER BY frequency ASC) >= 4 THEN 'Cannot Lose Them'
        WHEN NTILE(5) OVER (ORDER BY recency_days DESC) <= 2 
         AND NTILE(5) OVER (ORDER BY frequency ASC) <= 2 THEN 'Hibernating'
        ELSE 'Lost'
    END AS rfm_segment
FROM temp_rfm_base;




DROP TABLE IF EXISTS customer_churn_labels;

CREATE TABLE customer_churn_labels (
    customer_unique_id   STRING    COMMENT 'Unique customer identifier',
    last_order_date      DATE      COMMENT 'Date of last purchase',
    days_since_last_order INT      COMMENT 'Days since last purchase',
    is_churned           INT       COMMENT 'Churn flag (1=churned, 0=active)',
    churn_risk_level     STRING    COMMENT 'Risk level (High/Medium/Low)',
    total_orders         INT       COMMENT 'Lifetime order count',
    total_spend          DOUBLE    COMMENT 'Lifetime spend (BRL)'
)
COMMENT 'Customer churn labels. Business rule: churned if no purchase in last 60 days.'
STORED AS ORC
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY'
);

INSERT OVERWRITE TABLE customer_churn_labels
SELECT
    r.customer_unique_id,
    r.last_order_date,
    r.recency_days AS days_since_last_order,
    -- Churn label: 1 if no purchase in last 60 days
    CASE WHEN r.recency_days > 60 THEN 1 ELSE 0 END AS is_churned,
    -- Risk level
    CASE
        WHEN r.recency_days <= 30 THEN 'Low'
        WHEN r.recency_days <= 60 THEN 'Medium'
        WHEN r.recency_days <= 90 THEN 'High'
        ELSE 'Critical'
    END AS churn_risk_level,
    r.frequency AS total_orders,
    r.monetary AS total_spend
FROM rfm_features r;



-- Compute Statistics for Query Optimization
ANALYZE TABLE daily_kpi COMPUTE STATISTICS;
ANALYZE TABLE daily_kpi_by_category COMPUTE STATISTICS;
ANALYZE TABLE cohort_retention COMPUTE STATISTICS;
ANALYZE TABLE rfm_features COMPUTE STATISTICS;
ANALYZE TABLE customer_churn_labels COMPUTE STATISTICS;



SHOW TABLES IN gold;

-- Summary statistics
SELECT 'Daily KPIs' AS metric, COUNT(*) AS days FROM daily_kpi
UNION ALL
SELECT 'Total Cohorts', COUNT(DISTINCT cohort_month) FROM cohort_retention
UNION ALL
SELECT 'RFM Customers', COUNT(*) FROM rfm_features
UNION ALL
SELECT 'Churned Customers', SUM(is_churned) FROM customer_churn_labels;

-- Sample RFM segment distribution
SELECT rfm_segment, COUNT(*) AS customer_count
FROM rfm_features
GROUP BY rfm_segment
ORDER BY customer_count DESC;

-- Churn rate summary
SELECT
    is_churned,
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM customer_churn_labels
GROUP BY is_churned;
