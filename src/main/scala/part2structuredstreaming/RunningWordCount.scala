package part2structuredstreaming

import org.apache.spark.sql.{DataFrame, SparkSession}

object RunningWordCount extends App {

  val spark: SparkSession = SparkSession.builder()
    .appName("Running Word Count")
    .master("local[*]")
    .getOrCreate()

  // Importing implicits for encoders
  import spark.implicits._

  // For avoiding a lot of extra logs
  spark.sparkContext.setLogLevel("WARN")

  // Setting DataFrame to receive data from Socket
  val streamingSourceDF: DataFrame = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 12345)
    .load()

  // Transformations
  val transformedDF: DataFrame = streamingSourceDF.as[String]
    .flatMap(_.split(" "))
    .groupBy("value").count

  transformedDF.writeStream
    .format("console")
    .outputMode("complete")
    .start().awaitTermination()

}
