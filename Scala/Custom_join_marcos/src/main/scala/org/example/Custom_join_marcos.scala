package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr}

import scala.util.Random._

object Custom_join_marcos {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder.master("local[1]")
      .appName("DataSets")
      .config("spark.sql.debug.maxToStringFields", "200")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)


    val people_csv = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("sep", ";")
      .csv("src/data/people.csv")
      .select(col("name"), col("age"), col("number"))
    //people_csv.show(false)

    val numeros_csv = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("sep", ";")
      .csv("src/data/numeros.csv")
    //numeros_csv.show(false)

    //println(people_csv("number").rlike(numeros_csv("numero_texto").toString()))

    val custom_join_marcos = people_csv
      .join(numeros_csv, expr("number RLIKE numero_texto"), "inner")
      .drop(col("numero_texto"))
    custom_join_marcos.show(false)


    // Stop the SparkSession
    spark.stop()
  }
}
