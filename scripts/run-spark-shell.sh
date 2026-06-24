#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/common.sh"

ensure_spark
mkdir -p "$WAREHOUSE_DIR"

spark-shell \
  --master "${SPARK_MASTER:-local[*]}" \
  --packages "$(iceberg_packages)" \
  $(spark_log_conf) \
  $(spark_iceberg_conf)
