#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/common.sh"

ensure_spark
mkdir -p "$WAREHOUSE_DIR"

# Optional env vars:
#   CALENDAR_JOIN_MODE=full|2026     (default: full)
#   CALENDAR_JOIN_EVENTS=events|events_2026  (default: events)
#
# Usage: ./run-calendar-join.sh [full|2026] [events|events_2026] [warehouse]

CALENDAR_JOIN_MODE="${CALENDAR_JOIN_MODE:-full}"
CALENDAR_JOIN_EVENTS="${CALENDAR_JOIN_EVENTS:-events}"
WAREHOUSE_ARG="$WAREHOUSE_DIR"

for arg in "$@"; do
  case "$arg" in
    full|2026)
      CALENDAR_JOIN_MODE="$arg"
      ;;
    events|events_2026|evolved|uniform)
      CALENDAR_JOIN_EVENTS="$arg"
      ;;
    days|day|month_day|month-day)
      PARTITION_STRATEGY="$arg"
      ;;
    *)
      WAREHOUSE_ARG="$arg"
      ;;
  esac
done

export PARTITION_STRATEGY="${PARTITION_STRATEGY:-days}"

echo "Building application JAR..."
mvn -q -DskipTests package

spark-submit \
  --master "${SPARK_MASTER:-local[*]}" \
  --packages "$(iceberg_packages)" \
  $(spark_log_conf) \
  $(spark_iceberg_conf) \
  --class com.example.iceberg.DailyCalendarJoin \
  target/test-iceberg-proj-1.0-SNAPSHOT.jar \
  "$WAREHOUSE_ARG" \
  "$CALENDAR_JOIN_MODE" \
  "$CALENDAR_JOIN_EVENTS" \
  "$PARTITION_STRATEGY"
