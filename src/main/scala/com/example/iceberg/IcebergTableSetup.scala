package com.example.iceberg

import org.apache.spark.sql.SparkSession

object IcebergTableSetup {

  /** Evolve from months-only to days-only (not months + days). */
  def evolveMonthsToDaysOnly(spark: SparkSession, table: String): Unit = {
    spark.sql(
      f"ALTER TABLE $table REPLACE PARTITION FIELD months(created_at) WITH day(created_at)")
  }

  def createTimestampTable(
      spark: SparkSession,
      table: String,
      strategy: PartitionStrategy): Unit = {
    strategy match {
      case PartitionStrategy.DayOnly =>
        spark.sql(
          f"""
            |CREATE TABLE $table (
            |  created_at TIMESTAMP
            |) USING iceberg
            |PARTITIONED BY (days(created_at))
            |""".stripMargin)

      case PartitionStrategy.MonthDay =>
        spark.sql(
          f"""
            |CREATE TABLE $table (
            |  created_at TIMESTAMP
            |) USING iceberg
            |PARTITIONED BY (months(created_at))
            |""".stripMargin)
        spark.sql(f"ALTER TABLE $table ADD PARTITION FIELD day(created_at)")
    }
  }

  def createEventsTable(
      spark: SparkSession,
      table: String,
      strategy: PartitionStrategy): Unit = {
    strategy match {
      case PartitionStrategy.DayOnly =>
        spark.sql(
          f"""
            |CREATE TABLE $table (
            |  id BIGINT,
            |  event_type STRING,
            |  created_at TIMESTAMP
            |) USING iceberg
            |PARTITIONED BY (days(created_at))
            |""".stripMargin)

      case PartitionStrategy.MonthDay =>
        spark.sql(
          f"""
            |CREATE TABLE $table (
            |  id BIGINT,
            |  event_type STRING,
            |  created_at TIMESTAMP
            |) USING iceberg
            |PARTITIONED BY (months(created_at))
            |""".stripMargin)
        spark.sql(f"ALTER TABLE $table ADD PARTITION FIELD day(created_at)")
    }
  }

  def joinOnClause(strategy: PartitionStrategy): String = {
    val joinStyle = sys.env.getOrElse("JOIN_STYLE", "transform")
    strategy match {
      case PartitionStrategy.DayOnly if joinStyle.equalsIgnoreCase("timestamp") =>
        "c.created_at = e.created_at"
      case PartitionStrategy.DayOnly =>
        "local.system.days(c.created_at) = local.system.days(e.created_at)"
      case PartitionStrategy.MonthDay =>
        """local.system.months(c.created_at) = local.system.months(e.created_at)
          | AND local.system.days(c.created_at) = local.system.days(e.created_at)""".stripMargin
    }
  }

  def joinKeysDescription(strategy: PartitionStrategy): String = {
    val joinStyle = sys.env.getOrElse("JOIN_STYLE", "transform")
    strategy match {
      case PartitionStrategy.DayOnly if joinStyle.equalsIgnoreCase("timestamp") =>
        "created_at (timestamp equality)"
      case PartitionStrategy.DayOnly => "local.system.days(created_at)"
      case PartitionStrategy.MonthDay =>
        "local.system.months(created_at) and local.system.days(created_at)"
    }
  }
}
