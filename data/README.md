# Dataset Instructions

This project uses the **Olist Brazilian E-Commerce Public Dataset** from Kaggle.

## Dataset Download

1. Download the dataset from Kaggle:
   - URL: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

2. Extract the downloaded ZIP file to your local machine

## Required Files

The following CSV files are required for this project:

| File | Description | Records |
|------|-------------|---------|
| `olist_orders_dataset.csv` | Order header information | ~99k |
| `olist_customers_dataset.csv` | Customer demographics | ~99k |
| `olist_order_items_dataset.csv` | Order line items with prices | ~112k |
| `olist_products_dataset.csv` | Product catalog | ~32k |
| `olist_order_payments_dataset.csv` | Payment transactions | ~103k |
| `product_category_name_translation.csv` | Portuguese to English category names | 71 |

## Expected Directory Structure

Place the CSV files in a directory of your choice. The default expected path is:

```
~/datasets/olist/
├── olist_orders_dataset.csv
├── olist_customers_dataset.csv
├── olist_order_items_dataset.csv
├── olist_products_dataset.csv
├── olist_order_payments_dataset.csv
└── product_category_name_translation.csv
```

## Running Ingestion

Once the dataset is in place, run the ingestion script:

```bash
# Default path
./scripts/01_ingest_hdfs.sh ~/datasets/olist

# Or with a custom path
./scripts/01_ingest_hdfs.sh /path/to/your/olist/data
```

## Sample Data

The `data/sample/` directory contains small sample files (50 rows each) for testing and development purposes. These samples are included in the repository and can be used to verify the pipeline without downloading the full dataset.



- **Full datasets are NOT committed to this repository**
- Only sample data (50 rows) is included for testing
- Users must download and provide their own copy of the full dataset
