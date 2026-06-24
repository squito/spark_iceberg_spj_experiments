#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/common.sh"

ensure_spark
mkdir -p "$WAREHOUSE_DIR"

OUTPUT_FILE="${1:-spj-experiments-output.txt}"
: > "$OUTPUT_FILE"

run_experiment() {
  local name="$1"
  local calendar_strategy="$2"
  local partial_clustered="$3"
  local prefer_smj="$4"
  local join_style="${5:-transform}"
  local skip_order_by="${6:-false}"

  echo "================================================================" | tee -a "$OUTPUT_FILE"
  echo "Experiment: $name" | tee -a "$OUTPUT_FILE"
  echo "  CALENDAR_PARTITION_STRATEGY=$calendar_strategy" | tee -a "$OUTPUT_FILE"
  echo "  SPJ_PARTIAL_CLUSTERED=$partial_clustered" | tee -a "$OUTPUT_FILE"
  echo "  SPJ_PREFER_SORT_MERGE_JOIN=$prefer_smj" | tee -a "$OUTPUT_FILE"
  echo "  JOIN_STYLE=$join_style" | tee -a "$OUTPUT_FILE"
  echo "  SKIP_ORDER_BY=$skip_order_by" | tee -a "$OUTPUT_FILE"
  echo "================================================================" | tee -a "$OUTPUT_FILE"

  export PARTITION_STRATEGY="$calendar_strategy"
  export SPJ_PARTIAL_CLUSTERED="$partial_clustered"
  export SPJ_PREFER_SORT_MERGE_JOIN="$prefer_smj"
  export JOIN_STYLE="$join_style"
  export SKIP_ORDER_BY="$skip_order_by"

  echo "Building application JAR..."
  mvn -q -DskipTests package

  echo "Regenerating warehouse (events: months→days evolution; events_2026: days only)..."
  spark-submit \
    --master "${SPARK_MASTER:-local[*]}" \
    --packages "$(iceberg_packages)" \
    $(spark_log_conf) \
    $(spark_iceberg_conf) \
    --class com.example.iceberg.PartitionEvolutionDataGen \
    target/test-iceberg-proj-1.0-SNAPSHOT.jar \
    "$WAREHOUSE_DIR" \
    > /dev/null

  local run_log
  run_log="$(mktemp)"
  spark-submit \
    --master "${SPARK_MASTER:-local[*]}" \
    --packages "$(iceberg_packages)" \
    $(spark_log_conf) \
    $(spark_iceberg_conf) \
    --class com.example.iceberg.DailyCalendarJoin \
    target/test-iceberg-proj-1.0-SNAPSHOT.jar \
    "$WAREHOUSE_DIR" \
    "2026" \
    "events_2026" \
    "$calendar_strategy" \
    > "$run_log" 2>&1

  {
    echo "Join operator: $(grep 'Join operator:' "$run_log" || echo 'n/a')"
    echo "SPJ used: $(grep 'SPJ used:' "$run_log" || echo 'n/a')"
    echo "Reason: $(grep 'Reason:' "$run_log" || echo 'n/a')"
    echo "Left shuffle: $(grep 'Left input has pre-join shuffle:' "$run_log" || echo 'n/a')"
    echo "Right shuffle: $(grep 'Right input has pre-join shuffle:' "$run_log" || echo 'n/a')"
    echo "Calendar groupedBy: $(grep -A1 'BatchScan local.db.calendar_days' "$run_log" | grep groupedBy || echo 'n/a')"
    echo "Events groupedBy: $(grep -A1 'BatchScan local.db.events_2026' "$run_log" | grep groupedBy || echo 'n/a')"
    echo ""
  } | tee -a "$OUTPUT_FILE"

  rm -f "$run_log"
}

cd "$ROOT_DIR"

run_experiment "days calendar + transform join + partialClustered=true" "days" "true" "false"
run_experiment "days calendar + transform join + partialClustered=false" "days" "false" "false"
run_experiment "days calendar + timestamp join + partialClustered=true" "days" "true" "false" "timestamp" "false"
run_experiment "days calendar + timestamp join + preferSMJ=true" "days" "true" "true" "timestamp" "false"

echo "All experiments written to $OUTPUT_FILE"
