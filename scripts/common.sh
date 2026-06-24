#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

SPARK_VERSION="${SPARK_VERSION:-4.1.2}"
SCALA_VERSION="${SCALA_VERSION:-2.13}"
ICEBERG_VERSION="${ICEBERG_VERSION:-1.11.0}"
WAREHOUSE_DIR="${WAREHOUSE_DIR:-$ROOT_DIR/warehouse}"
SPARK_LOG_DIR="${SPARK_LOG_DIR:-$ROOT_DIR/logs}"

export SPARK_HOME="${SPARK_HOME:-$ROOT_DIR/.spark/spark-${SPARK_VERSION}-bin-hadoop3}"

spark_archive_valid() {
  local archive="$1"
  [[ -f "$archive" ]] && tar -tzf "$archive" >/dev/null 2>&1
}

ensure_spark() {
  export SPARK_HOME="$ROOT_DIR/.spark/spark-${SPARK_VERSION}-bin-hadoop3"
  export PATH="$SPARK_HOME/bin:$PATH"

  if [[ -x "$SPARK_HOME/bin/spark-submit" ]]; then
    return
  fi

  local archive="spark-${SPARK_VERSION}-bin-hadoop3.tgz"
  local url="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${archive}"
  local cache_dir="$ROOT_DIR/.spark"

  mkdir -p "$cache_dir"

  if ! spark_archive_valid "$cache_dir/$archive"; then
    echo "Downloading Spark ${SPARK_VERSION} (~400 MB, may take a few minutes)..."
    curl -fSL --continue-at - "$url" -o "$cache_dir/$archive"
    if ! spark_archive_valid "$cache_dir/$archive"; then
      echo "Spark download failed or archive is corrupt. Remove $cache_dir/$archive and retry." >&2
      exit 1
    fi
  fi

  if [[ ! -d "$SPARK_HOME" ]]; then
    echo "Extracting Spark ${SPARK_VERSION}..."
    tar -xzf "$cache_dir/$archive" -C "$cache_dir"
  fi

  export PATH="$SPARK_HOME/bin:$PATH"
}

iceberg_packages() {
  local spark_minor
  spark_minor="$(echo "$SPARK_VERSION" | sed -E 's/^([0-9]+)\.([0-9]+).*/\1.\2/')"
  echo "org.apache.iceberg:iceberg-spark-runtime-${spark_minor}_${SCALA_VERSION}:${ICEBERG_VERSION}"
}

spark_iceberg_conf() {
  echo "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
  echo "--conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog"
  echo "--conf spark.sql.catalog.local.type=hadoop"
  echo "--conf spark.sql.catalog.local.warehouse=${WAREHOUSE_DIR}"
}

spark_log_conf() {
  mkdir -p "$SPARK_LOG_DIR"
  local log4j_config="file://${ROOT_DIR}/conf/log4j2-spark.properties"
  echo "--conf spark.driver.extraJavaOptions=-Dlog4j2.configurationFile=${log4j_config}"
  echo "--conf spark.executor.extraJavaOptions=-Dlog4j2.configurationFile=${log4j_config}"
}
