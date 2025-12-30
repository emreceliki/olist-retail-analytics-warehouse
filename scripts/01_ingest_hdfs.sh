#!/bin/bash

set -e  # Exit on error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'


# Configuration
HDFS_BRONZE_PATH="/data/bronze/olist"

# Files and their target subdirectories (parallel arrays)
FILES=(
    "olist_orders_dataset.csv"
    "olist_customers_dataset.csv"
    "olist_order_items_dataset.csv"
    "olist_products_dataset.csv"
    "olist_order_payments_dataset.csv"
    "product_category_name_translation.csv"
)

SUBDIRS=(
    "orders"
    "customers"
    "order_items"
    "products"
    "payments"
    "category_translation"
)


log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

count_records() {
    local file=$1
    # Count lines minus header
    local count=$(($(wc -l < "$file") - 1))
    echo $count
}



# Check arguments
if [ -z "$1" ]; then
    log_error "Usage: $0 <dataset_path>"
    log_error "Example: $0 ~/datasets/olist"
    exit 1
fi

DATASET_PATH="$1"

# Expand tilde if present
DATASET_PATH="${DATASET_PATH/#\~/$HOME}"

log_info "Starting HDFS ingestion from: $DATASET_PATH"

# Validate dataset path exists
if [ ! -d "$DATASET_PATH" ]; then
    log_error "Dataset path does not exist: $DATASET_PATH"
    exit 1
fi

# Validate all required files exist
log_info "Validating required files..."
MISSING_FILES=()

for file in "${FILES[@]}"; do
    if [ ! -f "$DATASET_PATH/$file" ]; then
        MISSING_FILES+=("$file")
    fi
done

if [ ${#MISSING_FILES[@]} -ne 0 ]; then
    log_error "Missing required files:"
    for file in "${MISSING_FILES[@]}"; do
        log_error "  - $file"
    done
    exit 1
fi

log_info "All required files found."

# Create HDFS directory structure
log_info "Creating HDFS directories..."
docker exec namenode hdfs dfs -mkdir -p $HDFS_BRONZE_PATH

# Create subdirectories for each table
for subdir in "${SUBDIRS[@]}"; do
    docker exec namenode hdfs dfs -mkdir -p "$HDFS_BRONZE_PATH/$subdir"
done

# Upload each file to its subdirectory in HDFS
log_info "Uploading files to HDFS..."
echo ""

for i in "${!FILES[@]}"; do
    file="${FILES[$i]}"
    subdir="${SUBDIRS[$i]}"
    
    LOCAL_FILE="$DATASET_PATH/$file"
    HDFS_DIR="$HDFS_BRONZE_PATH/$subdir"
    HDFS_FILE="$HDFS_DIR/$file"
    
    # Count records
    RECORD_COUNT=$(count_records "$LOCAL_FILE")
    
    # Copy to container first (needed for Docker)
    log_info "Processing: $file -> $subdir/"
    docker cp "$LOCAL_FILE" namenode:/tmp/$file
    
    # Upload to HDFS (overwrite if exists)
    docker exec namenode hdfs dfs -put -f /tmp/$file $HDFS_FILE
    
    # Clean up temp file
    docker exec namenode rm /tmp/$file
    
    log_info "  ✓ Uploaded: $file ($RECORD_COUNT records)"
done

echo ""
log_info "------------------------------------------------"
log_info "Ingestion complete!"
log_info "------------------------------------------------"
echo ""

# Display HDFS contents
log_info "HDFS Bronze layer contents:"
docker exec namenode hdfs dfs -ls -R $HDFS_BRONZE_PATH

echo ""
log_info "Next steps:"
log_info "  1. Create Bronze tables: make bronze"
log_info "  2. Create Silver tables: make silver"
log_info "  3. Create Gold tables:   make gold"
