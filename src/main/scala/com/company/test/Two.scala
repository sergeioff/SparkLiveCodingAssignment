package com.company.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Two extends App {
  val spark = SparkSession.builder().master("local[*]").getOrCreate()

  import spark.implicits._

  val sessions = spark.read.parquet("data/session_data.parquet")
  val actions = spark.read.parquet("data/user_action_data.parquet")

  val notConvertedSessions = sessions
    .filter(not($"is_converted"))
    .select($"session_ts", $"session_uuid", explode($"struggle_types") as "struggle")

  val strippedActions = actions.select("session_uuid", "session_ts", "potential_revenue")
  val enrichedSessions = notConvertedSessions.join(strippedActions, Array("session_uuid", "session_ts"))

  enrichedSessions.groupBy($"session_uuid", $"session_ts", $"struggle.type", $"struggle.value")
    .agg(sum($"potential_revenue") as "potential_revenue")
    .withColumn("Date", to_date($"session_ts"))
    .drop($"session_ts")
    .withColumn("strugglesInSession", sum($"value").over(Window.partitionBy($"session_uuid")))
    .withColumn("Result", $"potential_revenue" * ($"value" / $"strugglesInSession"))
    .select($"Date" as "Day", $"type" as "Struggle", $"Result" as "Revenue loss")
    .show(false)
}
