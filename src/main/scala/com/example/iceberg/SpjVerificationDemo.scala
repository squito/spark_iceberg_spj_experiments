package com.example.iceberg

/**
 * Creates two Iceberg tables with identical bucket partitioning and joins on the
 * partition keys. With SPJ planning enabled, Spark should read both sides
 * co-partitioned and join without a pre-join shuffle.
 */
object SpjVerificationDemo {

  private val BucketCount = 8
  private val RowCount = 4000

  private val JoinQuery =
    """
      |SELECT
      |  l.id,
      |  l.category,
      |  l.left_value,
      |  r.right_value
      |FROM local.db.spj_left l
      |INNER JOIN local.db.spj_right r
      |  ON l.category = r.category AND l.id = r.id
      |ORDER BY l.id
      |""".stripMargin

  def main(args: Array[String]): Unit = {
    val warehousePath = args.headOption.getOrElse("warehouse")
    val spark = IcebergSpark.session("SpjVerificationDemo", warehousePath)

    try {
      spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")

      spark.sql("DROP TABLE IF EXISTS local.db.spj_left")
      spark.sql("DROP TABLE IF EXISTS local.db.spj_right")

      spark.sql(
        f"""
          |CREATE TABLE local.db.spj_left (
          |  id INT,
          |  category STRING,
          |  left_value INT
          |) USING iceberg
          |PARTITIONED BY (category, bucket($BucketCount, id))
          |""".stripMargin)

      spark.sql(
        f"""
          |CREATE TABLE local.db.spj_right (
          |  id INT,
          |  category STRING,
          |  right_value INT
          |) USING iceberg
          |PARTITIONED BY (category, bucket($BucketCount, id))
          |""".stripMargin)

      spark.sql(
        s"""
          |INSERT INTO local.db.spj_left
          |SELECT
          |  cast(id AS INT) AS id,
          |  case cast(id AS INT) % 4
          |    when 0 then 'A'
          |    when 1 then 'B'
          |    when 2 then 'C'
          |    else 'D'
          |  end AS category,
          |  cast(id AS INT) * 10 AS left_value
          |FROM range(0, $RowCount)
          |""".stripMargin)

      spark.sql(
        s"""
          |INSERT INTO local.db.spj_right
          |SELECT
          |  cast(id AS INT) AS id,
          |  case cast(id AS INT) % 4
          |    when 0 then 'A'
          |    when 1 then 'B'
          |    when 2 then 'C'
          |    else 'D'
          |  end AS category,
          |  cast(id AS INT) * 100 AS right_value
          |FROM range(0, $RowCount)
          |""".stripMargin)

      println(f"Inserted $RowCount rows into local.db.spj_left and local.db.spj_right")
      println("Partition specs:")
      spark.sql("SHOW CREATE TABLE local.db.spj_left").show(truncate = false)
      spark.sql("SHOW CREATE TABLE local.db.spj_right").show(truncate = false)

      println("Joining on category and id (matching bucket partition keys):")
      val joinDf = spark.sql(JoinQuery)
      val matchCount = joinDf.count()
      println(f"Join returned $matchCount rows")

      val analysis = SpjPlanAnalyzer.analyze(joinDf)
      SpjPlanAnalyzer.printAnalysis(analysis, joinDf)

      joinDf.show(5, false)
    } finally {
      spark.stop()
    }
  }
}
