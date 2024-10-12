package part2structuredstreaming

import common.stocksSchema
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

import scala.concurrent.duration.DurationInt

object StreamingDataFrames extends App {

  // Creating Spark Session
  val spark = SparkSession.builder()
    .appName("First Stream")
    .master("local[*]")
    .getOrCreate()

  // To stop so many INFO logs
  spark.sparkContext.setLogLevel("WARN")


  private def streamSocket(): Unit = {

    // Source -> Streaming Source
    val consoleDF: DataFrame = spark.readStream.format("socket")
      .option("host", "localhost")
      .option("port", "12345")
      .load()

    // Running a Streaming Query
    val streamingQuery: StreamingQuery = consoleDF.writeStream
      .format("console")
      .outputMode("append")
      .start()

    // Wait for it to complete
    streamingQuery.awaitTermination()

  }

  private def streamFile(): Unit = {

    // Streaming Source
    val streamFileDF: DataFrame = spark.readStream.format("csv")
      .option("header", "false")
      .option("dateFormat", "MMM d yyyy")
      .schema(stocksSchema)
      .load("src/main/resources/data/stocks")

    streamFileDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()

  }

  private def demoTriggers(): Unit = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "12345")
      .load()

    // Write the lines DF at a certain Trigger Point
    lines.writeStream
      .format("console")
      .outputMode("append")
      //.trigger(Trigger.ProcessingTime(2.seconds)) // Means Run the Query Every 2 seconds once arrives
      //.trigger(Trigger.Once()) // Single Batch and then terminate
      .trigger(Trigger.Continuous(2.seconds)) // experimental.. but spark will create batch regardless of if batch has
      // data or not
      .start().awaitTermination()

  }

  // Execute Function
  // streamSocket()
  // streamFile()
  demoTriggers()

}
