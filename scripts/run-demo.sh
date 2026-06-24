#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/common.sh"

ensure_spark
mkdir -p "$WAREHOUSE_DIR"

WAREHOUSE_ARG="$WAREHOUSE_DIR"
for arg in "$@"; do
  WAREHOUSE_ARG="$arg"
done

echo "Building application JAR..."
mvn -q -DskipTests package

spark-submit \
  --master "${SPARK_MASTER:-local[*]}" \
  --packages "$(iceberg_packages)" \
  $(spark_log_conf) \
  $(spark_iceberg_conf) \
  --class com.example.iceberg.PartitionEvolutionDataGen \
  target/test-iceberg-proj-1.0-SNAPSHOT.jar \
  "$WAREHOUSE_ARG"
