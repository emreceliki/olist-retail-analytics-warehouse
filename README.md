# Olist Retail Analytics Warehouse

Production-grade batch analytics data warehouse for e-commerce retention analysis using **HDFS** and **Apache Hive**.

![Hive](https://img.shields.io/badge/Apache%20Hive-FDEE21?style=for-the-badge&logo=apachehive&logoColor=black)
![Hadoop](https://img.shields.io/badge/Apache%20Hadoop-66CCFF?style=for-the-badge&logo=apachehadoop&logoColor=black)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)

## 📋 Overview

This project implements a complete **Bronze → Silver → Gold** data warehouse architecture for analyzing e-commerce customer behavior, including:

- **Cohort Analysis** - Monthly customer retention rates
- **RFM Segmentation** - Recency, Frequency, Monetary customer scoring
- **Churn Prediction Labels** - Binary classification based on 60-day inactivity
- **Daily KPIs** - GMV, AOV, order counts, unique customers

## 📦 Dataset

This project uses the **Olist Brazilian E-Commerce Public Dataset** from Kaggle:

🔗 **[https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)**

| Metric | Value |
|--------|-------|
| Orders | ~100K |
| Customers | ~99K |
| Products | ~32K |
| Time Range | 2016-2018 |
| Source | Real Brazilian e-commerce transactions |

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      Data Warehouse Layers                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐       │
│  │    BRONZE    │───▶│    SILVER    │───▶│     GOLD     │       │
│  │ (Raw/External│    │  (Curated)   │    │  (Analytics) │       │
│  │    Tables)   │    │              │    │              │       │
│  └──────────────┘    └──────────────┘    └──────────────┘       │
│         │                   │                   │               │
│         ▼                   ▼                   ▼               │
│  • CSV SerDe         • ORC + Snappy      • daily_kpi            │
│  • External tables   • Date partitions   • cohort_retention     │
│  • No transforms     • Deduplication     • rfm_features         │
│                      • Type casting      • churn_labels         │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │
┌─────────────────────────────────────────────────────────────────┐
│                        Infrastructure                           │
├─────────────────────────────────────────────────────────────────┤
│  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌───────────┐  │
│  │  NameNode  │  │  DataNode  │  │   Hive     │  │ PostgreSQL│  │
│  │   (HDFS)   │  │   (HDFS)   │  │  Server2   │  │ (Metastore│  │
│  │  :9870     │  │            │  │  :10000    │  │   :5433)  │  │
│  └────────────┘  └────────────┘  └────────────┘  └───────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

---

## 📊 Data Model

### Bronze Layer (Raw)
| Table | Description |
|-------|-------------|
| `orders_raw` | Order header information |
| `customers_raw` | Customer demographics |
| `order_items_raw` | Order line items with prices |
| `products_raw` | Product catalog |
| `payments_raw` | Payment transactions |
| `category_translation_raw` | Portuguese → English mapping |

### Silver Layer (Curated)
| Table | Description | Storage |
|-------|-------------|---------|
| `orders` | Cleaned orders, partitioned by `dt` | ORC + Snappy |
| `customers` | Deduplicated customers | ORC + Snappy |
| `order_items` | Items with calculated totals | ORC + Snappy |
| `products` | Products with English categories | ORC + Snappy |
| `payments` | Aggregated payments per order | ORC + Snappy |

### Gold Layer (Analytics)
| Table | Description | Key Metrics |
|-------|-------------|-------------|
| `daily_kpi` | Daily business metrics | Order count, GMV, AOV, unique customers |
| `daily_kpi_by_category` | Category-level KPIs | GMV by product category |
| `cohort_retention` | Monthly cohort analysis | Retention rates (M0, M1, M2...) |
| `rfm_features` | Customer RFM scores | R/F/M scores (1-5), segment labels |
| `customer_churn_labels` | Churn classification | Binary churn flag (60-day rule) |

---

## 🚀 Running the Project (Step-by-Step)

### Prerequisites
- Docker
- Dataset downloaded to `~/datasets/olist/`

---

### Step 1: Start Containers

```bash
# Navigate to project directory
cd olist-retail-analytics-warehouse

# Start with Make
make up

# OR directly with Docker Compose:
docker-compose up -d
```

**This command starts:**
| Service | Port | Description |
|---------|------|-------------|
| NameNode | 9870 | HDFS Web UI |
| DataNode | - | Data storage |
| HiveServer2 | 10000 | Query engine |
| Hive Metastore | 9083 | Table metadata |
| PostgreSQL | 5433 | Metastore DB |

⏱️ **Wait ~90 seconds** (for services to initialize)

```bash
# Check container status
make status

# OR
docker-compose ps
```

---

### Step 2: Load Data to HDFS

```bash
# Load with Make (default path: ~/datasets/olist)
make ingest

# OR run the script directly
./scripts/01_ingest_hdfs.sh ~/datasets/olist
```

**This step:**
- Validates required CSV files exist
- Creates `/data/bronze/olist/` directory in HDFS
- Uploads all CSV files to HDFS
- Logs record count for each file

---

### Step 3: Create Bronze Layer (Raw Data)

```bash
# With Make
make bronze

# OR directly with Beeline
docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000 -f /scripts/02_create_bronze.hql
```

**This step:**
- Creates `bronze` database
- Defines external Hive tables for CSV files
- No data transformation (raw data as-is)

---

### Step 4: Create Silver Layer (Cleaned Data)

```bash
# With Make
make silver

# OR directly with Beeline
docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000 -f /scripts/03_create_silver.hql
```

**This step:**
- Creates `silver` database
- Casts data types (STRING → TIMESTAMP, DOUBLE, etc.)
- Removes duplicate records
- Handles NULL values
- Compresses with **ORC + Snappy** format
- Partitions `orders` table by `dt` (date)

---

### Step 5: Create Gold Layer (Analytics Tables)

```bash
# With Make
make gold

# OR directly with Beeline
docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000 -f /scripts/04_create_gold.hql
```

**Created analytics tables:**

| Table | Description |
|-------|-------------|
| `daily_kpi` | Daily sales metrics (GMV, AOV, order count) |
| `daily_kpi_by_category` | Category-level daily KPIs |
| `cohort_retention` | Monthly customer retention analysis |
| `rfm_features` | Customer RFM segmentation (1-5 scores) |
| `customer_churn_labels` | Churn prediction (60-day rule) |

---

### Step 6: Run Data Quality Checks

```bash
# With Make
make quality

# OR directly with Beeline
docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000 -f /scripts/05_quality_checks.hql
```

**Validates:**
- NULL values
- Duplicate primary keys
- Invalid dates
- Negative amounts
- Referential integrity

---

### Run Full Pipeline with One Command

```bash
# Execute all steps in sequence
make all
```

---

### Step 7: Query Data

```bash
# Open Hive CLI (Beeline)
make beeline

# OR
docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000
```

---

### Stopping the Project

```bash
# Stop containers (data preserved)
make down

# OR
docker-compose down

# Stop containers AND delete all data
make clean

# OR
docker-compose down -v
```

---

## 📁 Project Structure

```
olist-retail-analytics-warehouse/
├── docker-compose.yml     
├── hadoop.env             
├── Makefile              
├── README.md             
│
├── data/
│   ├── README.md        
│   └── sample/          
│
├── scripts/
│   ├── 01_ingest_hdfs.sh  
│   ├── 02_create_bronze.hql 
│   ├── 03_create_silver.hql 
│   ├── 04_create_gold.hql  
│   └── 05_quality_checks.hql 
│
└── docs/
    ├── architecture.md    
    └── benchmarks.md     
```

---

## ⚙️ Technical Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| **Storage Format** | ORC | Columnar, predicate pushdown, efficient compression |
| **Compression** | Snappy | Fast decompression, good compression ratio |
| **Partitioning** | Date (`dt`) | Common query pattern, efficient partition pruning |
| **Metastore DB** | PostgreSQL | Production-ready, reliable |
| **Churn Window** | 60 days | E-commerce industry standard for inactivity |

---

## 📈 Example Queries

### Daily KPIs
```sql
SELECT dt, order_count, gmv, avg_order_value
FROM gold.daily_kpi
WHERE dt BETWEEN '2018-01-01' AND '2018-01-31'
ORDER BY dt;
```

### Cohort Retention Matrix
```sql
SELECT 
    cohort_month,
    months_since_cohort,
    ROUND(retention_rate * 100, 1) AS retention_pct
FROM gold.cohort_retention
WHERE cohort_month = '2017-01'
ORDER BY months_since_cohort;
```

### Customer Segmentation Distribution
```sql
SELECT 
    rfm_segment,
    COUNT(*) AS customers,
    ROUND(AVG(monetary), 2) AS avg_spend
FROM gold.rfm_features
GROUP BY rfm_segment
ORDER BY customers DESC;
```

### Churn Analysis
```sql
SELECT 
    is_churned,
    COUNT(*) AS customers,
    ROUND(AVG(total_spend), 2) AS avg_lifetime_value
FROM gold.customer_churn_labels
GROUP BY is_churned;
```

---

## 📄 License

MIT License

---

## 🙏 Acknowledgments

- [Olist](https://olist.com/) for providing the public dataset
- [Big Data Europe](https://github.com/big-data-europe) for Docker images
