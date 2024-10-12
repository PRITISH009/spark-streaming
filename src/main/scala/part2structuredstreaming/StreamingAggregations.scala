package part2structuredstreaming

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.concurrent.duration.DurationInt

object StreamingAggregations extends App {
  val spark = SparkSession.builder()
    .appName("Streaming Aggregation")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  def streamingCount() = {
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    val lineCount: DataFrame = lines.coalesce(1).selectExpr("count(*) as lineCount")

    // aggregations with distinct are not supported
    // otherwise spark will need to keep track of everything

    lineCount.writeStream
      .format("console")
      .outputMode("complete") // append and update not supported on aggregations without watermarks*
      .trigger(Trigger.Continuous(5.seconds))
      .start().awaitTermination()

  }

  def numericalAggregations(aggFunction: Column => Column): Unit = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    val numbers = lines.select(col("value").cast("integer").as("number"))
    val sumDF = numbers.select(aggFunction(col("number")).as("agg_so_far"))

    sumDF.writeStream
      .format("console")
      .outputMode("complete")
      .start().awaitTermination()
  }

  // Grouping
  def groupNames(): Unit = {
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    val names = lines
      .select(col("value").as("name"))
      .groupBy(col("name"))
      .count()

    names.writeStream
      .format("console")
      .outputMode("complete")
      .start().awaitTermination()

  }

  // streamingCount()
  // numericalAggregations(sum)
  // numericalAggregations(mean)
  groupNames()
}
