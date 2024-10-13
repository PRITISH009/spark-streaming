package part2structuredstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object StreamingJoins extends App {
  val spark = SparkSession.builder()
    .appName("Streaming Joins")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  // Static DFs
  val guitarPlayers = spark.read.option("inferSchema", true).json("src/main/resources/data/guitarPlayers")
  val guitars = spark.read.option("inferSchema", true).json("src/main/resources/data/guitars")
  val bands = spark.read.option("inferSchema", true).json("src/main/resources/data/bands")

  // Join Guitar Players with Band
  val joinCondition = guitarPlayers.col("band") === bands.col("id")
  val guitaristsBands = guitarPlayers.join(bands, joinCondition, "inner")
  val bandsSchema = bands.schema

  def joinStreamWithStatic() = {
    val streamedBandsDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load() // DF with a single column "value" of type string
      .select(from_json(col("value"), bandsSchema).as("band"))
      .selectExpr("band.id as id", "band.name as name", "band.hometown as hometown", "band.year as year")

    // The join happens per batch

    /*
      Restricted Joins:
        - stream joining with static: Right outer join/full outer join/ right_semi not permitted
        - static joining with streaming: LEFT outer join/full/left_semi not permitted

        Reason -> Spark will need to keep track of all incoming data.
     */

    // Since Spark2.3 we have stream vs stream joins

    val streamedBandsGuitaristsDF = streamedBandsDF.join(guitarPlayers,
      guitarPlayers.col("band") === streamedBandsDF.col("id"), "left")  // inner by default

    streamedBandsGuitaristsDF.writeStream.format("console")
      .outputMode("append") // append is supported for joins (with some restrictions)
      .start().awaitTermination()
  }

  def joinStreamWithStream() = {
    val streamedBandsDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .select(from_json(col("value"), bandsSchema).as("band"))
      .selectExpr("band.id as id", "band.name as name", "band.hometown as hometown", "band.year as year")

    val streamingGuitaristDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12346)
      .load()
      .select(from_json(col("value"), guitarPlayers.schema).as("guitarPlayer"))
      .selectExpr("guitarPlayer.id as id", "guitarPlayer.name as name", "guitarPlayer.guitars as guitars", "guitarPlayer.band as band")

    val streamedJoin = streamedBandsDF.join(streamingGuitaristDF,
      streamedBandsDF.col("id") === streamingGuitaristDF.col("band")) // default inner

    /*
      For stream some other joins are supported -
        - inner joins are supported
        - left/right outer joins are supported, but MUST have watermarks
        - full outer joins are NOT supported
     */
    streamedJoin.writeStream
      .format("console")
      .outputMode("append") // Only Append supported for stream vs stream joining
      .start().awaitTermination()

  }

  joinStreamWithStatic()
//  joinStreamWithStream()
}
