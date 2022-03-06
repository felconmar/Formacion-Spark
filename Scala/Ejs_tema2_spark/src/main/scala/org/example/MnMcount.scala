package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MnMcount {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder.master("local[1]")
      .appName("MnMCount")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    // Get the M&M data set filename
    val mnmFile = "src/data/mm_dataset.csv"

    // Read the file into a Spark DataFrame
    val mnmDF = spark.read.option("header", true).csv(mnmFile)
    // Aggregate counts of all colors and groupBy() State and Color
    // orderBy() in descending order
    val countMnMDF = mnmDF
      .select("State", "Color", "Count")
      .groupBy("State", "Color")
      .agg(count("Count").alias("Total"))
      .orderBy(desc("Total"))
    // Show the resulting aggregations for all the states and colors
    countMnMDF.show(60)
    println(s"Total Rows = ${countMnMDF.count()}")
    println()
    // Find the aggregate counts for California by filtering
    val caCountMnMDF = mnmDF
      .select("State", "Color", "Count")
      .where(col("State") === "CA")
      .groupBy("State", "Color")
      .agg(count("Count").alias("Total"))
      .orderBy(desc("Total"))
    // Show the resulting aggregations for California
    caCountMnMDF.show(10)
    // Stop the SparkSession
    spark.stop()
  }
}