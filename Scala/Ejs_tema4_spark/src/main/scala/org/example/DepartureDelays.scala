package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.source.image


object DepartureDelays {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder.master("local[1]")
      .appName("Flights")
      .config("spark.sql.debug.maxToStringFields", "200")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

    val dd_path = "src/data/departuredelays.csv"

    val flights = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(dd_path)
    //Create temp view
    flights.createOrReplaceTempView("flights_view")

    //Flights with distance over 1000 miles (get all rows tip)
    spark.sql("select * from flights_view where distance > 1000 order by distance DESC").show(flights.count().toInt, false)
    val mil_dist = spark.sql("select * from flights_view where distance > 1000 order by distance DESC")
    mil_dist.show(10, false)

    //Flights between SFO and ORD with at least 2 hour delay
    val sfo_ord_flights = spark.sql("select * from flights_view where origin == 'SFO' " +
      "and destination == 'ORD' and delay >= 120 order by delay DESC")
    sfo_ord_flights.show(10, false)

    //label all US flights, regardless of origin and destination,
    //with an indication of the delays they experienced: Very Long Delays (> 6 hours),
    //Long Delays (2–6 hours), etc.
    val human_read_delays = spark.sql("select date, origin, destination, delay, " +
      "CASE when delay >= 360 then 'Very Long Delay' " +
      "when delay < 360 and delay >= 120 then 'Long Delay' " +
      "when delay < 120 and delay > 0 then 'Short Delay' " +
      "when delay = 0 then 'No delay' " +
      "else 'Good for u' " +
      "END AS ReadableDelay " +
      "from flights_view order by delay DESC")
    human_read_delays.show(10, false)
    //Create a new database
    spark.sql("CREATE DATABASE learn_spark_db")
    //Use db
    spark.sql("use learn_spark_db")
    //Create table on that db

    spark.sql("""CREATE TABLE us_delay_flights_tbl(date STRING, delay INT,
                      distance INT, origin STRING, destination STRING)
                      USING csv OPTIONS (PATH
                      'src/data/departuredelays.csv')""")

    //GlobalTempView vs TempView: GlobalTempViews can be accessed by any SparkSession within a Spark application.
    //                            A TempView can only be accessed by the SparkSession that created it.

    //Accesing spark metadata
    spark.catalog.listDatabases().show(false)
    spark.catalog.listTables().show(false)
    spark.catalog.listColumns("us_delay_flights_tbl").show(false)

    //DataFrameWriter: access its instance through the dataframe you want to save
    //---Parquet

    flights.write.format("parquet")
      .mode("overwrite")
      .option("compression", "snappy")
      .save("src/data/exports/parquet/flights.parquet")
    //---SQLTable
    flights.write
      .mode("overwrite")
      .saveAsTable("sql_flights_table")

    //---JSON
    flights.write.format("json")
      .mode("overwrite")
      //.option("compression", "snappy")
      .save("src/data/exports/json/flights.json")

    //---CSV
    flights.write.format("csv")
      .mode("overwrite")
      .save("src/data/exports/csv/flights.csv")

    //---AVRO
    flights.write.format("avro")
      .mode("overwrite")
      .save("src/data/exports/avro/flights.avro")

    //---ORC
    flights.write.format("orc")
      .mode("overwrite")
      .save("src/data/exports/orc/flights.orc")





    //DataFrameReader: access through a SparkSession instance
    //---Parquet
    val parquet_read_flights = spark.read.format("parquet").load("src/data/exports/parquet/flights.parquet/")
    parquet_read_flights.show(5)
    //---SQLTable
    spark.sql("select * from sql_flights_table").show()
    //---JSON
    val json_read_flights = spark.read.format("json").load("src/data/exports/json/flights.json")
    json_read_flights.show(5)
    //---CSV
    val csv_read_flights = spark.read.format("csv").load("src/data/exports/csv/flights.csv")
    csv_read_flights.show(5)
    //---AVRO
    val avro_read_flights = spark.read.format("avro").load("src/data/exports/avro/flights.avro")
    avro_read_flights.show(5)
    //---ORC
    val orc_read_flights = spark.read.format("orc").load("src/data/exports/orc/flights.orc")
    orc_read_flights.show(5)
    //---Images
    val imagesDF = spark.read.format("image").load("src/data/train_images/")
    imagesDF.printSchema()
    imagesDF.select("image.height", "image.width", "image.nChannels", "image.mode", "label").show(5, false)
    //---BinaryFile
    val binary_read_flights = spark.read.format("binaryFile")
      .option("pathGlobalFilter", "*.jpg")
      .option("recursiveFileLookup", "true")
      .load("src/data/train_images/")
    binary_read_flights.show(5)

    // Stop the SparkSession
    spark.stop()
  }
}
