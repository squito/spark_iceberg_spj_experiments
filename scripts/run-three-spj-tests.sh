#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/common.sh"

export SPARK_VERSION="${SPARK_VERSION:-4.1.2}"
export SCALA_VERSION="${SCALA_VERSION:-2.13}"
export ICEBERG_VERSION="${ICEBERG_VERSION:-1.11.0}"
export JOIN_STYLE=timestamp
export SPJ_PARTIAL_CLUSTERED=true
export SPJ_PREFER_SORT_MERGE_JOIN=false
export PARTITION_STRATEGY=days

OUTPUT_DIR="${1:-$ROOT_DIR/spark-4.1-tests}"
mkdir -p "$OUTPUT_DIR"

ensure_spark
mkdir -p "$WAREHOUSE_DIR"

echo "Spark: $(spark-submit --version 2>&1 | head -1 || true)"
echo "Iceberg package: $(iceberg_packages)"
echo "Regenerating warehouse..."
mvn -q -DskipTests package
bash "$ROOT_DIR/scripts/run-demo.sh" "$WAREHOUSE_DIR" > "$OUTPUT_DIR/demo-output.txt" 2>&1

run_test() {
  local name="$1"
  local mode="$2"
  local events="$3"
  local outfile="$OUTPUT_DIR/${name}.txt"

  echo "Running $name..."
  bash "$ROOT_DIR/scripts/run-calendar-join.sh" "$WAREHOUSE_DIR" "$mode" "$events" days \
    > "$outfile" 2>&1

  {
    echo "=== $name ==="
    echo "Spark ${SPARK_VERSION}, Iceberg ${ICEBERG_VERSION}"
    grep -E "(Calendar range|Events table|Join returned|SPJ used|Reason|pre-join shuffle|groupedBy)" "$outfile" || true
    echo
  } | tee -a "$OUTPUT_DIR/summary.txt"
}

: > "$OUTPUT_DIR/summary.txt"

run_test "test1-events_2026-2026" "2026" "events_2026"
run_test "test2-events-2026" "2026" "events"
run_test "test3-events-full" "full" "events"

echo "Results written to $OUTPUT_DIR/summary.txt"
