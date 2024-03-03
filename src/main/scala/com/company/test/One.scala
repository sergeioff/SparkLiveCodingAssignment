package com.company.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object One extends App {
  val spark = SparkSession.builder().master("local[*]").getOrCreate()

  import spark.implicits._

  val pageLoads = spark.read.parquet("data/page_load_data.parquet")
  val userActions = spark.read.parquet("data/user_action_data.parquet")

  val userDataAggregation = userActions.groupBy($"session_uuid")
    .agg(
      count(when($"client_action" === "click", true)) as "ClickCount",
      countDistinct($"dom_element") as "NumberOfDomElements",
      sum(when($"potential_revenue" > 0 and $"client_action" === "click", 1)) as "PotentialRevenueClicks"
    )

  val numPageLoads = pageLoads
    .groupBy($"session_uuid")
    .agg(count($"session_uuid") as "NumberOfPageLoads")

  userDataAggregation
    .join(numPageLoads, "session_uuid")
    .show(false)
}
