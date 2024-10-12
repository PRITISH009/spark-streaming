package part2structuredstreaming

import common.stocksSchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

object StreamingDataFrames {

  // Step 1: Create a spark session -
  val spark = SparkSession.builder()
    .appName("Our First Streams")
    .master("local[*]")
    .getOrCreate()

  def readFromSocket() = {
    // Dataframes can also be streaming dataframes
    val lines: DataFrame = spark.readStream.format("socket")
      .option("host", "localhost")
      .option("port", "12345")
      .load()

    val shortLines = lines.where(length(col("value")) <= 10)

    // How to tell if a data is static of streaming
    println(shortLines.isStreaming)

    // Defining A sink and starting the sink
    val query: StreamingQuery = shortLines.writeStream
      .format("console")
      .outputMode("append")
      .start()

    query.awaitTermination()
  }

  def readFromFile() = {
    val stocksDF = spark.readStream
      .format("csv")
      .option("header", "false")
      .option("dateFormat", "MMM d yyyy")
      .schema(stocksSchema)
      .load("src/main/resources/stocks")
  }

  def main(args: Array[String]): Unit = {
    readFromSocket()
  }

}
