package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import java.util.concurrent.atomic.AtomicInteger


object Quijote {
  def main(args : Array[String]) {
    val spark:SparkSession = SparkSession.builder().master("local[1]")
      .appName("quijote")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

    val quijote_data = spark.sparkContext
      .textFile("src/data/mnm_dataset.csv")
    val nlineas = quijote_data.count()
    println("Número de líneas: " + nlineas)
    val take = quijote_data.take(5)
    println("Take: ")
    take.foreach(unit=>println(unit))
    val first = quijote_data.first()
    println("First: " + first)
    val auto_id  = new AtomicInteger(1)
    val quijote_map = quijote_data.map(linea=>Row(auto_id.getAndIncrement(), linea))
    println("Map:")
    quijote_map.take(5).foreach(unit=>println(unit))
    val schema = StructType(List(
      StructField("ID", IntegerType, true),
      StructField("linea", StringType, true)
    ))
    val quijote_df = spark.sqlContext.createDataFrame(quijote_map, schema)
    println("Head DF: " + quijote_df.head(5))
    println("First DF: " + quijote_df.first())
    println("Show without truncate: ")
    quijote_df.show(10)
    println("Show with truncate: ")
    quijote_df.show(10, truncate = 5)

  }
}