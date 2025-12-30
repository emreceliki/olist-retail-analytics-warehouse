# Architecture Documentation

## System Architecture Overview

This document provides a detailed overview of the Olist Retail Analytics Warehouse architecture.

## Infrastructure Components

### Docker Services

| Service | Image | Ports | Purpose |
|---------|-------|-------|---------|
| **namenode** | bde2020/hadoop-namenode | 9870, 9000 | HDFS NameNode - manages file system namespace |
| **datanode** | bde2020/hadoop-datanode | - | HDFS DataNode - stores actual data blocks |
| **hive-metastore** | bde2020/hive | 9083 | Hive Metastore - schema registry |
| **hive-server** | bde2020/hive | 10000, 10002 | HiveServer2 - query interface |
| **postgres** | postgres:13 | 5433 | PostgreSQL - metastore backend |

### Network Topology

```
                          ┌─────────────────┐
                          │  Docker Network │
                          │  (hadoop-net)   │
                          └────────┬────────┘
                                   │
       ┌───────────────────────────┼───────────────────────────┐
       │                           │                           │
       ▼                           ▼                           ▼
┌─────────────┐            ┌─────────────┐            ┌─────────────┐
│  NameNode   │◀──────────▶│  DataNode   │            │  PostgreSQL │
│  (HDFS)     │    HDFS    │  (HDFS)     │            │  (Metastore)│
└─────────────┘  Protocol  └─────────────┘            └──────┬──────┘
       ▲                                                     │ JDBC
       │                                                     │
       │  HDFS                                               ▼
       │                                              ┌─────────────┐
       ├─────────────────────────────────────────────▶│    Hive     │
       │                                              │  Metastore  │
       │                                              └──────┬──────┘
       │                                                     │ Thrift
       │                                                     ▼
       │                                              ┌─────────────┐
       └─────────────────────────────────────────────▶│ HiveServer2 │
                                                      │  (Beeline)  │
                                                      └─────────────┘
```

## Data Flow

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   CSV Files  │────▶│     HDFS     │────▶│    Bronze    │────▶│    Silver    │
│(Local/Kaggle)│ sh  │ /data/bronze │ HQL │  External    │ HQL │   ORC + dt   │
└──────────────┘     └──────────────┘     └──────────────┘     └──────┬───────┘
                                                                      │
                                                                      │ HQL
                                                                      ▼
                                                               ┌──────────────┐
                                                               │     Gold     │
                                                               │  Analytics   │
                                                               └──────────────┘
```

## Layer Details

### Bronze Layer
- **Location:** `/data/bronze/olist/`
- **Format:** CSV (external tables)
- **Purpose:** Raw data ingestion without transformation
- **Tables:** 6 (orders, customers, order_items, products, payments, category_translation)

### Silver Layer
- **Location:** `/user/hive/warehouse/silver.db/`
- **Format:** ORC with Snappy compression
- **Purpose:** Cleaned, typed, deduplicated data
- **Partitioning:** `orders` table partitioned by `dt` (YYYY-MM-DD)
- **Tables:** 5 (orders, customers, order_items, products, payments)

### Gold Layer
- **Location:** `/user/hive/warehouse/gold.db/`
- **Format:** ORC with Snappy compression
- **Purpose:** Business-ready analytics tables
- **Tables:** 5 (daily_kpi, daily_kpi_by_category, cohort_retention, rfm_features, customer_churn_labels)

## Storage Optimization

### Why ORC?
1. **Columnar Storage** - Only reads required columns
2. **Predicate Pushdown** - Filters at storage level
3. **Built-in Compression** - Snappy for fast decompression
4. **Statistics** - Min/max values for query optimization

### Partitioning Strategy
- **Partition Column:** `dt` (date string YYYY-MM-DD)
- **Benefit:** Partition pruning reduces I/O by 90%+ for date-filtered queries
- **Example:** `WHERE dt = '2018-01-15'` only scans one partition

## Query Optimization

### Statistics Collection
```sql
ANALYZE TABLE silver.orders COMPUTE STATISTICS FOR COLUMNS;
```

### EXPLAIN Plan Example
```sql
EXPLAIN SELECT COUNT(*) FROM silver.orders WHERE dt = '2018-01-15';
-- Shows partition pruning in action
```
