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
      .withColumn("id",monotonically_increasing_id)
      .withColumn("Size", col("Size").cast("int"))
    //nasa_logs_parsed_1.show(false)
    val nasa_logs_parsed_2 = nasa_logs_parsed_1.select(col("_Host_"),col("Date"), col("HTTP_status_code"), col("Size"), col("Request_Method/Resource/Protocol"))
      .withColumn("Request_Method", regexp_extract(col("Request_Method/Resource/Protocol"), """^(.*?)\s""", 1))
      .withColumn("Resource", regexp_extract(col("Request_Method/Resource/Protocol"), """\s(.+?(?=\s|$))""", 1))
      .withColumn("Protocol", regexp_extract(col("Request_Method/Resource/Protocol"), """\s(HTTP.+)""", 1))
      .withColumn("Protocol", when(length(col("Protocol"))===0, "NO_PROTOCOL")
        .otherwise(col("Protocol")))
      .withColumn("Date", to_timestamp(col("Date"),"dd/MMM/yyyy:HH:mm:ss"))
      .drop(col("Request_Method/Resource/Protocol"))

    //- ¿Cuáles son los distintos protocolos web utilizados? Agrúpalos.
    nasa_logs_parsed_2
      .groupBy("Protocol")
      .count()
      //.show(false)
    //- ¿Cuáles son los códigos de estado más comunes en la web? Agrúpalos y ordénalos
    //   para ver cuál es el más común.
    nasa_logs_parsed_2
      .groupBy("HTTP_status_code")
      .count()
      .orderBy(col("count"))
      //.show(false)
    //- ¿Y los métodos de petición (verbos) más utilizados?
    nasa_logs_parsed_2
      .groupBy("Request_Method")
      .count()
      .orderBy(col("count"))
      //.show(false)
    //- ¿Qué recurso tuvo la mayor transferencia de bytes de la página web?
    nasa_logs_parsed_2
      .groupBy("Resource")
      .sum("Size")
      .orderBy(desc("sum(Size)"))
      //.show(false)
    //  - Además, queremos saber que recurso de nuestra web es el que más tráfico recibe. Es
    //  decir, el recurso con más registros en nuestro log.
    nasa_logs_parsed_2
      .groupBy("Resource")
      .count()
      .orderBy(desc("count"))
      //.show(false)
    // - ¿Qué días la web recibió más tráfico?
    nasa_logs_parsed_2
      .withColumn("Simple_Date", to_date(col("Date")))
      .groupBy(col("Simple_date"))
      .count()
      .orderBy(desc("count"))
      //.show(false)
    //- ¿Cuáles son los hosts son los más frecuentes?
    nasa_logs_parsed_2
      .groupBy(col("_Host_"))
      .count()
      .orderBy(desc("count"))
      //.show(false)
    //- ¿A qué horas se produce el mayor número de tráfico en la web?
    nasa_logs_parsed_2
      .withColumn("Hours", hour(col("Date")))
      .groupBy(col("Hours"))
      .count()
      .orderBy(desc("count"))
      //.show(false)
    // - ¿Cuál es el número de errores 404 que ha habido cada día?
    nasa_logs_parsed_2
      .where(col("HTTP_status_code")==="404")
      .withColumn("Simple_Date", to_date(col("Date")))
      .groupBy(col("Simple_date"))
      .count()
      .orderBy(desc("count"))
      .select(col("Simple_Date"), col("count").alias("404_errors_per_day"))
      //.show(false)


    // Stop the SparkSession
    spark.stop()
  }
}
