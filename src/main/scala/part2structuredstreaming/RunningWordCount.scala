package part2structuredstreaming

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object RunningWordCount extends App {

  val spark: SparkSession = SparkSession.builder()
    .appName("Running Word Count")
    .master("local[*]")
    .getOrCreate()

  // For avoiding a lot of extra logs
  spark.sparkContext.setLogLevel("WARN")

  // Setting DataFrame to receive data from Socket
  val streamingSourceDF: DataFrame = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 12345)
    .load()

  // Transformations
  val transformedDF: DataFrame = streamingSourceDF.filter(length(col("value")) <= 10)

  // Defining Stream Output and Starting the stream
  transformedDF.writeStream
    .outputMode("complete")
    .format("console")
    .start().awaitTermination()
}
