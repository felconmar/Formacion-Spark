package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, length, regexp_extract}
import org.apache.spark.sql.functions._

import scala.util.Random._

object Nasa_logs {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder.master("local[1]")
      .appName("DataSets")
      .config("spark.sql.debug.maxToStringFields", "200")
      .getOrCreate()


    //regexp_extract


    import spark.implicits._

    val nasa_logs_txt = spark.read.text("src/data/nasalogs/access_log_*").where(!col("value").contains("400 -"))

    val nasa_logs_parsed_1 = nasa_logs_txt.select(regexp_extract(col("value"), """^([^(\s|,)]+)""", 1).alias("host"),
      regexp_extract(col("value"), """^([^(\s|,)]+)""", 1).alias("_Host_"),
      regexp_extract(col("value"), """\[([^\s+]+)""", 1).alias("Date"),
      regexp_extract(col("value"), """"((?:(?!\s\"|\"|From).)*)""", 1).alias("Request_Method/Resource/Protocol"),
      regexp_extract(col("value"), """\s(\d+)\s""", 1).alias("HTTP_status_code"),
      regexp_extract(col("value"), """\s(\d+)$""", 1).alias("Size"))
      .where(length(col("Date")) > 1)
      .withColumn("id",monotonically_increasing_id)
      .withColumn("Size", col("Size").cast("int"))
    val nasa_logs_parsed_2 = nasa_logs_parsed_1.select(col("_Host_"),col("Date"), col("HTTP_status_code"), col("Size"), col("Request_Method/Resource/Protocol"))
      .withColumn("Request_Method", regexp_extract(col("Request_Method/Resource/Protocol"), """^(.*?)\s""", 1))
      .withColumn("Resource", regexp_extract(col("Request_Method/Resource/Protocol"), """\s(.+?(?=\s|$))""", 1))
      .withColumn("Protocol", regexp_extract(col("Request_Method/Resource/Protocol"), """\s(HTTP.+)""", 1))
      .withColumn("Protocol", when(length(col("Protocol"))===0, "NO_PROTOCOL")
        .otherwise(col("Protocol")))
      .withColumn("Date", to_timestamp(col("Date"),"dd/MMM/yyyy:HH:mm:ss"))
      .drop(col("Request_Method/Resource/Protocol"))
    /*
    nasa_logs_parsed_2.write.format("parquet")
      .mode("overwrite")
      .option("compression", "snappy")
      .save("src/data/exports/parquet/nasa_logs_clean.parquet")
    */
    
    val nasa_logs_parquet = spark.read.format("parquet").load("src/data/exports/parquet/nasa_logs_clean.parquet")

    println("??Cu??les son los distintos protocolos web utilizados? Agr??palos.")
    nasa_logs_parquet
      .groupBy("Protocol")
      .count()
      .show(false)
    println("==============================================================================================")
    println("??Cu??les son los c??digos de estado m??s comunes en la web? " +
      "Agr??palos y ord??nalos para ver cu??l es el m??s com??n.")
    nasa_logs_parquet
      .groupBy("HTTP_status_code")
      .count()
      .orderBy(desc("count"))
      .show(false)
    println("==============================================================================================")
    println("??Y los m??todos de petici??n (verbos) m??s utilizados?")
    nasa_logs_parquet
      .groupBy("Request_Method")
      .count()
      .orderBy(desc("count"))
      .show(false)
    println("==============================================================================================")
    println("??Qu?? recurso tuvo la mayor transferencia de bytes de la p??gina web?")
    nasa_logs_parquet
      .groupBy("Resource")
      .sum("Size")
      .orderBy(desc("sum(Size)"))
      .show(1,false)
    println("==============================================================================================")
    println("Adem??s, queremos saber que recurso de nuestra web es el que m??s tr??fico recibe. " +
      "Es decir, el recurso con m??s registros en nuestro log.")
    nasa_logs_parquet
      .groupBy("Resource")
      .count()
      .orderBy(desc("count"))
      .show(1,false)
    println("==============================================================================================")
    println("??Qu?? d??as la web recibi?? m??s tr??fico?")
    nasa_logs_parquet
      .withColumn("Simple_Date", to_date(col("Date")))
      .groupBy(col("Simple_date"))
      .count()
      .orderBy(desc("count"))
      .show(10,false)
    println("==============================================================================================")
    println("??Cu??les son los hosts son los m??s frecuentes?")
    nasa_logs_parquet
      .groupBy(col("_Host_"))
      .count()
      .orderBy(desc("count"))
      .show(false)
    println("==============================================================================================")
    println("??A qu?? horas se produce el mayor n??mero de tr??fico en la web?")
    nasa_logs_parquet
      .withColumn("Hours", hour(col("Date")))
      .groupBy(col("Hours"))
      .count()
      .orderBy(desc("count"))
      .show(25,false)
    println("==============================================================================================")
    println("??Cu??l es el n??mero de errores 404 que ha habido cada d??a?")
    nasa_logs_parquet
      .where(col("HTTP_status_code")==="404")
      .withColumn("Simple_Date", to_date(col("Date")))
      .groupBy(col("Simple_date"))
      .count()
      .orderBy(desc("count"))
      .select(col("Simple_Date"), col("count").alias("404_errors_per_day"))
      .show(60,false)
    println("==============================================================================================")

    // Stop the SparkSession
    spark.stop()
  }
}
