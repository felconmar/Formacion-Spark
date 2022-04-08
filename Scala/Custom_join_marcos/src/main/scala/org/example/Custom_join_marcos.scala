package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr}

import scala.util.Random._

object Custom_join_marcos {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder.master("local[1]")
      .appName("DataSets")
      .config("spark.sql.debug.maxToStringFields", "200")
      .getOrCreate()



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

    //"#$%FEWGTRES%SDF&%=       >  TRES
    //println(people_csv("number").rlike(numeros_csv("numero_texto").toString()))

    val custom_join_marcos = people_csv
      .join(numeros_csv, expr("number RLIKE numero_texto"), "inner")
      .drop(col("numero_texto"))
    custom_join_marcos.show(false)


    val custom_join_marcos_2 = people_csv
      .joinWith(numeros_csv, col("number").contains(col("numero_texto")))
      .withColumn("Name", col("_1").getItem("name"))
      .withColumn("Age", col("_1").getItem("age"))
      .withColumn("Código", col("_1").getItem("number"))
      .withColumn("Número", col("_2").getItem("numero"))
      .drop(col("_1"))
      .drop(col("_2"))
    custom_join_marcos_2.show(false)

    //regexp_extract

    // Stop the SparkSession
    spark.stop()
  }
}
