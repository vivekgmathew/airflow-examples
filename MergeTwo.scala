package wk

import org.apache.spark.sql.expressions.Window

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object MergeTwo extends App {
  val spark = SparkSession.builder().
    appName("DataSources and Formats").
    config("spark.master", "local").
    getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import org.apache.spark.sql.functions.current_timestamp

  println("Current Master")
  val masterData = Seq(
    Row(1, "111"),
    Row(2, "222"),
    Row(3, "333"),
    Row(4, "444"),
    Row(5, "555"),
    Row(9, "999")
  )

  val masterSchema = List(
    StructField("number", IntegerType, true),
    StructField("word", StringType, true)
  )
  val dfMaster = spark.createDataFrame(spark.sparkContext.parallelize(masterData), StructType(masterSchema))

  import org.apache.spark.sql.functions.current_timestamp
  dfMaster.show(false)


  val dailyData = Seq(
    Row(1, "100"),
    Row(2, "200"),
    Row(5, "555"),
    Row(6, "666"),
    Row(7, "777"),
    Row(9, "999")
  )

  val dfDaily = spark.createDataFrame(spark.sparkContext.parallelize((dailyData)), StructType(masterSchema))
  val upserts = dfMaster.join(dfDaily, Seq("number", "word"), "leftanti")
  println("Upserts")
  val upsertDfTs = upserts.withColumn("time_stamp", current_timestamp())
  upsertDfTs.cache()
  upsertDfTs.show(false)

  println("Todays Daily")
  val dfDailyTs = dfDaily.withColumn("time_stamp", current_timestamp())
  dfDailyTs.cache()
  dfDailyTs.show(false)

  println("Merged")
  val merged = upsertDfTs.union(dfDailyTs)
  merged.show(false)

  // Remove duplicates using timestamp information
  import org.apache.spark.sql.functions._
  val win = Window.partitionBy("number").orderBy(col("time_stamp").desc)
  val finalMerged = merged.withColumn("rn", row_number().over(win))
                          .where("rn=1")
                          .drop(col("rn"))
  println(finalMerged)
  finalMerged.show(false)
}
