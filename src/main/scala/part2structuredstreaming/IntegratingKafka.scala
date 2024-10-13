package part2structuredstreaming

import common._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object IntegratingKafka extends App {
  val spark = SparkSession.builder()
    .appName("Integrating Kafka")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  def readFromKafka() = {
    val initDF = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "dummyTopic")
      .load()

    initDF
      .select(col("topic"), expr("cast(value as String)"))
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def writeToKafka() = {
    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")

    carsDF.selectExpr("upper(Name) as key", "Name as value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "dummyTopic")
      .option("checkpointLocation", "checkpoints") // without checkpoints, the writing will fail
      .start()
      .awaitTermination()

  }

  def writeToKafkaAsJsonString(): Unit = {
    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")

    val carsJsonKafkaDF = carsDF.select(col("Name").as("key"),
      to_json(struct(col("Name"), col("Horsepower"), col("Origin"))).as("value"))

    carsJsonKafkaDF
      .writeStream
      .format("kafka")
      .option("topic", "dummyTopic")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("checkpointLocation", "checkpoints")
      .start()
      .awaitTermination()

  }

//  readFromKafka()
//  writeToKafka()

  writeToKafkaAsJsonString()
}
