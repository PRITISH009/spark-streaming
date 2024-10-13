package part2structuredstreaming

import common.{Car, carsSchema}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._

object StreamingDatasets extends App {
  val spark = SparkSession.builder()
    .appName("Streaming Datasets")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

   import spark.implicits._

  def readCars(): Dataset[Car] = {
    val carEncoder = Encoders.product[Car]
    spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .select(from_json(col("value"), carsSchema).as("car"))
      .selectExpr("car.*")
      .as[Car](carEncoder)
  }

  def showCarNames(): Unit = {
    val carsDS: Dataset[Car] = readCars()

    val carNamesDS: DataFrame = carsDS.select("Name") // Returns DataFrame

    // collection transformation maintain type info
    val altCarNamesDS: Dataset[String] = carsDS.map(_.Name) // Returns Dataset

    altCarNamesDS.writeStream
      .format("console")
      .outputMode("append")
      .start().awaitTermination()
  }

  //showCarNames()

  /*
    Exercises ->
      1. count cars having 140HP (powerful cars)
      2. Average HP for entire Dataset
      3. Count the cars by their origin field
   */

  def countCars(): Unit = {
    spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .select(from_json(col("value"), carsSchema).as("cars"))
      .selectExpr("cars.*")
      .as[Car]
      .filter(_.Horsepower.getOrElse(0l) > 140l)
      .writeStream
      .format("console")
      .outputMode("append")
      .start().awaitTermination()
  }

//  countCars()

  def avgHP() = {
    spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .select(from_json(col("value"), carsSchema).as("cars"))
      .selectExpr("cars.*")
      .as[Car]
      .select(avg(col("Horsepower")))
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()
      .awaitTermination()
  }

//  avgHP()

  def countCarsByOrigin() = {
    spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .select(from_json(col("value"), carsSchema).as("cars"))
      .selectExpr("cars.*")
      .as[Car]
      .groupBy(col("Origin"))
      .count()
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def countCarsAltByOrigin() = {
    spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .select(from_json(col("value"), carsSchema).as("cars"))
      .selectExpr("cars.*")
      .as[Car]
      .groupByKey(_.Origin).count()
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()

  }

  countCarsAltByOrigin()
}
