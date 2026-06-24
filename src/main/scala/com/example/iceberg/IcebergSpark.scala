package com.example.iceberg

import org.apache.spark.sql.SparkSession

object IcebergSpark {

  def session(appName: String, warehousePath: String): SparkSession = {
    val partiallyClustered = sys.env.getOrElse("SPJ_PARTIAL_CLUSTERED", "true")
    val preferSortMergeJoin = sys.env.getOrElse("SPJ_PREFER_SORT_MERGE_JOIN", "false")

    SparkSession.builder()
      .appName(appName)
      .master(sys.props.getOrElse("spark.master", "local[*]"))
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.local.type", "hadoop")
      .config("spark.sql.catalog.local.warehouse", warehousePath)
      .config("spark.sql.sources.v2.bucketing.enabled", "true")
      .config("spark.sql.iceberg.planning.preserve-data-grouping", "true")
      .config("spark.sql.sources.v2.bucketing.pushPartValues.enabled", "true")
      .config("spark.sql.requireAllClusterKeysForCoPartition", "false")
      .config("spark.sql.sources.v2.bucketing.partiallyClusteredDistribution.enabled", partiallyClustered)
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .config("spark.sql.join.preferSortMergeJoin", preferSortMergeJoin)
      .getOrCreate()
  }
}
