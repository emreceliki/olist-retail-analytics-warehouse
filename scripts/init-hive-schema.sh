#!/bin/bash

set -e

echo "Checking if Hive metastore schema exists..."

echo "Waiting for PostgreSQL to be ready..."
while ! nc -z postgres 5432 2>/dev/null; do
    sleep 2
done
echo "PostgreSQL is ready!"

sleep 5

echo "Attempting schema initialization..."
/opt/hive/bin/schematool -dbType postgres -info > /dev/null 2>&1 || {
    echo "Schema not found. Initializing Hive metastore schema..."
    /opt/hive/bin/schematool -dbType postgres -initSchema
    echo "Schema initialization completed!"
}

echo "Hive metastore schema is ready."
echo "Starting Hive Metastore..."
exec /opt/hive/bin/hive --service metastore
