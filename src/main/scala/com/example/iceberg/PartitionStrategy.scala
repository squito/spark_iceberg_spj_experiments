package com.example.iceberg

sealed trait PartitionStrategy {
  def label: String
}

object PartitionStrategy {

  /** Single hidden partition: days(created_at) */
  case object DayOnly extends PartitionStrategy {
    val label = "days(created_at)"
  }

  /** Calendar table with months + day (legacy; not used for events). */
  case object MonthDay extends PartitionStrategy {
    val label = "months(created_at), days(created_at)"
  }

  def parse(arg: String): PartitionStrategy =
    if (arg.equalsIgnoreCase("days") || arg.equalsIgnoreCase("day")) DayOnly else MonthDay

  def isStrategyArg(arg: String): Boolean =
    arg.equalsIgnoreCase("days") ||
      arg.equalsIgnoreCase("day") ||
      arg.equalsIgnoreCase("month_day") ||
      arg.equalsIgnoreCase("month-day")

  def fromEnvOrArg(args: Array[String]): PartitionStrategy = {
    args.find(isStrategyArg)
      .map(parse)
      .orElse(sys.env.get("PARTITION_STRATEGY").map(parse))
      .getOrElse(DayOnly)
  }
}
