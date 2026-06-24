package com.example.iceberg

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, SortMergeJoinExec, ShuffledHashJoinExec}

case class SpjAnalysis(
    joinOperator: String,
    storagePartitionedJoinUsed: Boolean,
    reason: String,
    leftSubtreeHasShuffle: Option[Boolean] = None,
    rightSubtreeHasShuffle: Option[Boolean] = None)

object SpjPlanAnalyzer {

  def analyze(joinDf: DataFrame): SpjAnalysis = {
    val plan = finalizedPlan(joinDf.queryExecution.executedPlan)

    findJoin(plan) match {
      case Some(smj: SortMergeJoinExec) =>
        val leftShuffle = subtreeHasShuffleExchange(smj.left)
        val rightShuffle = subtreeHasShuffleExchange(smj.right)
        val spjUsed = !leftShuffle && !rightShuffle
        SpjAnalysis(
          joinOperator = "SortMergeJoin",
          storagePartitionedJoinUsed = spjUsed,
          reason = if (spjUsed) {
            "No shuffle Exchange nodes appear before the join; data was read co-partitioned."
          } else {
            "SortMergeJoin with pre-join shuffle Exchange on at least one side."
          },
          leftSubtreeHasShuffle = Some(leftShuffle),
          rightSubtreeHasShuffle = Some(rightShuffle))

      case Some(_: BroadcastHashJoinExec) =>
        SpjAnalysis(
          joinOperator = "BroadcastHashJoin",
          storagePartitionedJoinUsed = false,
          reason = "Spark broadcast a small table instead of co-partitioning both sides.")

      case Some(_: ShuffledHashJoinExec) =>
        SpjAnalysis(
          joinOperator = "ShuffledHashJoin",
          storagePartitionedJoinUsed = false,
          reason = "Shuffled hash join always repartitions both sides.")

      case None =>
        SpjAnalysis(
          joinOperator = "Unknown",
          storagePartitionedJoinUsed = false,
          reason = "No join operator found in the executed physical plan.")
    }
  }

  def printAnalysis(analysis: SpjAnalysis, joinDf: DataFrame): Unit = {
    println("Storage Partitioned Join (SPJ) analysis:")
    println(f"  Join operator: ${analysis.joinOperator}")
    println(f"  SPJ used: ${if (analysis.storagePartitionedJoinUsed) "YES" else "NO"}")
    println(f"  Reason: ${analysis.reason}")
    analysis.leftSubtreeHasShuffle.foreach { hasShuffle =>
      println(f"  Left input has pre-join shuffle: $hasShuffle")
    }
    analysis.rightSubtreeHasShuffle.foreach { hasShuffle =>
      println(f"  Right input has pre-join shuffle: $hasShuffle")
    }
    println("Relevant physical plan excerpt:")
    joinDf.explain("formatted")
  }

  private def finalizedPlan(plan: SparkPlan): SparkPlan = plan match {
    case adaptive: AdaptiveSparkPlanExec =>
      val finalPlan = adaptive.finalPhysicalPlan
      if (finalPlan != null) finalPlan else adaptive.executedPlan
    case other => other
  }

  private def findJoin(plan: SparkPlan): Option[SparkPlan] = plan match {
    case join: SortMergeJoinExec => Some(join)
    case join: BroadcastHashJoinExec => Some(join)
    case join: ShuffledHashJoinExec => Some(join)
    case adaptive: AdaptiveSparkPlanExec => findJoin(finalizedPlan(adaptive))
    case stage: QueryStageExec => findJoin(stage.plan)
    case node => node.children.flatMap(findJoin).headOption
  }

  private def subtreeHasShuffleExchange(plan: SparkPlan): Boolean = plan match {
    case _: ShuffleExchangeExec => true
    case _: BroadcastExchangeExec => false
    case stage: QueryStageExec => subtreeHasShuffleExchange(stage.plan)
    case adaptive: AdaptiveSparkPlanExec => subtreeHasShuffleExchange(finalizedPlan(adaptive))
    case node => node.children.exists(subtreeHasShuffleExchange)
  }
}
