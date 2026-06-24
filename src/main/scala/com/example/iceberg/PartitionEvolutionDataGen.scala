package com.example.iceberg

import java.io.File

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object PartitionEvolutionDataGen {

  private val RecordsPerMonth = 1000
  private val DaysPerMonth = 28
  private val Years = 2024 to 2025
  private val Months = 1 to 12
  private val PostEvolutionYear = 2026
  private val PostEvolutionMonths = 1 to 6

  private val EventsTable = "local.db.events"
  private val Events2026Table = "local.db.events_2026"

  private def clearWarehouse(warehousePath: String): Unit = {
    val warehouseDir = new File(warehousePath)
    if (warehouseDir.exists()) {
      deleteRecursively(warehouseDir)
      println(f"Cleared warehouse at ${warehouseDir.getAbsolutePath}")
    } else {
      println(f"Warehouse path does not exist yet: ${warehouseDir.getAbsolutePath}")
    }
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursively)
    }
    if (!file.delete()) {
      throw new IllegalStateException(f"Failed to delete ${file.getAbsolutePath}")
    }
  }

  private def insertMonthRecords(spark: SparkSession, table: String, year: Int, month: Int): Unit = {
    val eventsDf = spark.range(RecordsPerMonth)
      .withColumnRenamed("id", "seq")
      .withColumn("id", lit(year) * 1000000L + lit(month) * 10000L + col("seq"))
      .withColumn(
        "event_type",
        when(col("seq") % 3 === 0, "click")
          .when(col("seq") % 3 === 1, "view")
          .otherwise("purchase"))
      .withColumn(
        "day",
        (col("seq") * lit(DaysPerMonth) / lit(RecordsPerMonth)).cast("int") + lit(1))
      .withColumn(
        "created_at",
        make_timestamp(lit(year), lit(month), col("day"), lit(0), lit(0), lit(0)))
      .select("id", "event_type", "created_at")

    eventsDf.writeTo(table).append()
  }

  private def generateEvolvedEventsTable(spark: SparkSession): Int = {
    spark.sql(
      f"""
        |CREATE TABLE $EventsTable (
        |  id BIGINT,
        |  event_type STRING,
        |  created_at TIMESTAMP
        |) USING iceberg
        |PARTITIONED BY (months(created_at))
        |""".stripMargin)

    for (year <- Years; month <- Months) {
      insertMonthRecords(spark, EventsTable, year, month)
    }

    IcebergTableSetup.evolveMonthsToDaysOnly(spark, EventsTable)

    for (month <- PostEvolutionMonths) {
      insertMonthRecords(spark, EventsTable, PostEvolutionYear, month)
    }

    Years.size * Months.size * RecordsPerMonth + PostEvolutionMonths.size * RecordsPerMonth
  }

  private def generateUniform2026EventsTable(spark: SparkSession): Int = {
    IcebergTableSetup.createEventsTable(spark, Events2026Table, PartitionStrategy.DayOnly)

    for (month <- PostEvolutionMonths) {
      insertMonthRecords(spark, Events2026Table, PostEvolutionYear, month)
    }

    PostEvolutionMonths.size * RecordsPerMonth
  }

  def main(args: Array[String]): Unit = {
    val warehousePath = args.headOption.getOrElse("warehouse")
    clearWarehouse(warehousePath)

    val spark = SparkSession.builder()
      .appName("PartitionEvolutionDataGen")
      .master(sys.props.getOrElse("spark.master", "local[*]"))
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.local.type", "hadoop")
      .config("spark.sql.catalog.local.warehouse", warehousePath)
      .getOrCreate()

    try {
      spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")

      val evolvedTotal = generateEvolvedEventsTable(spark)
      val initialCount = Years.size * Months.size * RecordsPerMonth
      val postEvolutionCount = PostEvolutionMonths.size * RecordsPerMonth
      println(f"Table $EventsTable (partition evolution):")
      println(
        f"  Inserted $initialCount records (${RecordsPerMonth} per month for ${Years.min}-${Years.max})")
      println(
        f"  Inserted $postEvolutionCount records (${RecordsPerMonth} per month for $PostEvolutionYear-01 through $PostEvolutionYear-06 after months→days evolution)")
      println(f"  Total: $evolvedTotal records")

      val uniform2026Total = generateUniform2026EventsTable(spark)
      println(f"Table $Events2026Table (2026 only, ${PartitionStrategy.DayOnly.label} partitioning):")
      println(
        f"  Inserted $uniform2026Total records (${RecordsPerMonth} per month for $PostEvolutionYear-01 through $PostEvolutionYear-06)")

      println(f"Records per month ($EventsTable):")
      spark.sql(
        f"""
          |SELECT
          |  year(created_at) AS year,
          |  month(created_at) AS month,
          |  count(*) AS record_count
          |FROM $EventsTable
          |WHERE year(created_at) BETWEEN 2024 AND 2026
          |GROUP BY year(created_at), month(created_at)
          |ORDER BY year, month
          |""".stripMargin).show(30, false)

      println(f"Records per month ($Events2026Table):")
      spark.sql(
        f"""
          |SELECT
          |  year(created_at) AS year,
          |  month(created_at) AS month,
          |  count(*) AS record_count
          |FROM $Events2026Table
          |GROUP BY year(created_at), month(created_at)
          |ORDER BY year, month
          |""".stripMargin).show(10, false)

      println(f"Partition spec ($EventsTable):")
      spark.sql(f"SHOW CREATE TABLE $EventsTable").show(truncate = false)

      println(f"Partition spec ($Events2026Table):")
      spark.sql(f"SHOW CREATE TABLE $Events2026Table").show(truncate = false)
    } finally {
      spark.stop()
    }
  }
}
