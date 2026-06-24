package com.example.iceberg

import org.apache.spark.sql.functions._

sealed trait CalendarJoinMode {
  def startDate: String
  def endDate: String
  def description: String
}

object CalendarJoinMode {

  case object Full extends CalendarJoinMode {
    val startDate = "2024-01-01"
    val endDate = "2026-06-30"
    val description = "full range (2024-01-01 through 2026-06-30; mixed event partition specs)"
  }

  case object Year2026 extends CalendarJoinMode {
    val startDate = "2026-01-01"
    val endDate = "2026-06-30"
    val description = "2026 only (Jan–Jun)"
  }

  def parse(arg: String): CalendarJoinMode =
    if (arg.equalsIgnoreCase("2026")) Year2026 else Full

  def isModeArg(arg: String): Boolean =
    arg.equalsIgnoreCase("full") || arg.equalsIgnoreCase("2026")
}

sealed trait EventsSource {
  def tableName: String
  def description: String
}

object EventsSource {

  case object Evolved extends EventsSource {
    val tableName = "local.db.events"
    val description = "local.db.events (partition evolution, 2024–2026)"
  }

  case object Uniform2026 extends EventsSource {
    val tableName = "local.db.events_2026"
    val description = "local.db.events_2026 (2026 only, uniform partitioning)"
  }

  def parse(arg: String): EventsSource =
    if (arg.equalsIgnoreCase("events_2026") || arg.equalsIgnoreCase("uniform")) {
      Uniform2026
    } else {
      Evolved
    }

  def isEventsSourceArg(arg: String): Boolean =
    arg.equalsIgnoreCase("events") ||
      arg.equalsIgnoreCase("events_2026") ||
      arg.equalsIgnoreCase("evolved") ||
      arg.equalsIgnoreCase("uniform")
}

object DailyCalendarJoin {

  private def joinQuery(eventsTable: String, strategy: PartitionStrategy): String = {
    val joinOn = IcebergTableSetup.joinOnClause(strategy)
    val skipOrderBy = sys.env.getOrElse("SKIP_ORDER_BY", "false").equalsIgnoreCase("true")
    val orderClause = if (skipOrderBy) "" else "ORDER BY year, month, day, e.id"
    f"""
      |SELECT
      |  year(local.system.days(c.created_at)) AS year,
      |  month(local.system.days(c.created_at)) AS month,
      |  day(local.system.days(c.created_at)) AS day,
      |  e.id,
      |  e.event_type,
      |  e.created_at AS event_at
      |FROM local.db.calendar_days c
      |JOIN $eventsTable e
      |  ON $joinOn
      |$orderClause
      |""".stripMargin
  }

  private def isConfigArg(arg: String): Boolean =
    CalendarJoinMode.isModeArg(arg) ||
      EventsSource.isEventsSourceArg(arg) ||
      PartitionStrategy.isStrategyArg(arg)

  private def parseArgs(args: Array[String]): (String, CalendarJoinMode, EventsSource, PartitionStrategy) = {
    val modeArg = args.find(CalendarJoinMode.isModeArg)
    val eventsArg = args.find(EventsSource.isEventsSourceArg)
    val warehousePath = args.find(!isConfigArg(_)).getOrElse("warehouse")
    val mode = modeArg.map(CalendarJoinMode.parse).getOrElse(CalendarJoinMode.Full)
    val eventsSource = eventsArg.map(EventsSource.parse).getOrElse(EventsSource.Evolved)
    val partitionStrategy = PartitionStrategy.fromEnvOrArg(args)
    (warehousePath, mode, eventsSource, partitionStrategy)
  }

  def main(args: Array[String]): Unit = {
    val (warehousePath, mode, eventsSource, partitionStrategy) = parseArgs(args)
    val spark = IcebergSpark.session("DailyCalendarJoin", warehousePath)

    try {
      println(f"Calendar range: ${mode.description}")
      println(f"Events table: ${eventsSource.description}")
      println(f"Partition strategy: ${partitionStrategy.label}")
      println(f"SPJ partial clustered: ${sys.env.getOrElse("SPJ_PARTIAL_CLUSTERED", "true")}")
      println(f"Prefer sort-merge join: ${sys.env.getOrElse("SPJ_PREFER_SORT_MERGE_JOIN", "false")}")

      spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")

      val calendarDf = spark.sql(
        f"""
          |SELECT to_timestamp(d) AS created_at
          |FROM (
          |  SELECT explode(sequence(date '${mode.startDate}', date '${mode.endDate}', interval 1 day)) AS d
          |)
          |""".stripMargin)

      spark.sql("DROP TABLE IF EXISTS local.db.calendar_days")
      IcebergTableSetup.createTimestampTable(spark, "local.db.calendar_days", partitionStrategy)

      val months = spark.sql(
        f"""
          |SELECT explode(sequence(date '${mode.startDate}', date '${mode.endDate}', interval 1 month)) AS month_start
          |""".stripMargin)

      months.collect().foreach { row =>
        val monthStart = row.getDate(0)
        val monthDf = calendarDf.filter(
          year(col("created_at")) === year(lit(monthStart)) &&
            month(col("created_at")) === month(lit(monthStart)))
        monthDf.writeTo("local.db.calendar_days").append()
      }

      val dayCount = calendarDf.count()
      println(
        f"Inserted $dayCount calendar records (one per day from ${mode.startDate} through ${mode.endDate})")

      println("Sample calendar rows:")
      spark.table("local.db.calendar_days").orderBy("created_at").show(5, false)

      println("Partition spec (calendar):")
      spark.sql("SHOW CREATE TABLE local.db.calendar_days").show(truncate = false)

      println(f"Partition spec (${eventsSource.tableName}):")
      spark.sql(f"SHOW CREATE TABLE ${eventsSource.tableName}").show(truncate = false)

      println(f"Joined rows (no aggregation) from ${eventsSource.tableName}:")
      println(f"Join keys: ${IcebergTableSetup.joinKeysDescription(partitionStrategy)}")
      println(f"JOIN_STYLE: ${sys.env.getOrElse("JOIN_STYLE", "transform")}")
      println(f"SKIP_ORDER_BY: ${sys.env.getOrElse("SKIP_ORDER_BY", "false")}")

      val joinDf = spark.sql(joinQuery(eventsSource.tableName, partitionStrategy))
      val joinCount = joinDf.count()
      println(f"Join returned $joinCount rows")
      SpjPlanAnalyzer.printAnalysis(SpjPlanAnalyzer.analyze(joinDf), joinDf)
      joinDf.show(20, false)
    } finally {
      spark.stop()
    }
  }
}
