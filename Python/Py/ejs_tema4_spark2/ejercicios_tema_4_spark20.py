import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

if __name__ == "__main__":
    flights_path = os.path.realpath(os.path.join(os.path.dirname(__file__), '../../..', 'data', 'departuredelays.csv'))

    # Create SparkSession
    spark = (SparkSession
             .builder
             .appName("Flights")
             .config('spark.sql.debug.maxToStringFields', '200')
             .getOrCreate())

    flights = (spark.read.format("csv")
               .option("inferSchema", "true")
               .option("header", "true")
               .load(flights_path))
    # Create temp view
    flights.createOrReplaceTempView("flights_view")

    # Flights with distance over 1000 miles (get all rows tip)
    spark.sql("select * from flights_view where distance > 1000 order by distance DESC").show(flights.count(),
                                                                                              False)
    mil_dist = spark.sql("select * from flights_view where distance > 1000 order by distance DESC")
    mil_dist.show(10, False)

    # Flights between SFO and ORD with at least 2 hour delay
    sfo_ord_flights = spark.sql("select * from flights_view where origin == 'SFO' " + \
                                "and destination == 'ORD' and delay >= 120 order by delay DESC")
    sfo_ord_flights.show(10, False)

    # label all US flights, regardless of origin and destination,
    # with an indication of the delays they experienced: VeryLongDelays( > 6 hours),
    # LongDelays(2–6 hours), etc.
    human_read_delays = spark.sql("select date, origin, destination, delay, " +
                                  "CASE when delay >= 360 then 'Very Long Delay' " +
                                  "when delay < 360 and delay >= 120 then 'Long Delay' " +
                                  "when delay < 120 and delay > 0 then 'Short Delay' " +
                                  "when delay = 0 then 'No delay' " +
                                  "else 'Good for u' " +
                                  "END AS ReadableDelay " +
                                  "from flights_view order by delay DESC")
    human_read_delays.show(10, False)
    # Create a new database
    spark.sql("CREATE DATABASE learn_spark_db")
    # Use db
    spark.sql("use learn_spark_db")
    # Create table on that db
    spark.sql("""CREATE TABLE us_delay_flights_tbl(date STRING, delay INT,
                          distance INT, origin STRING, destination STRING)
                          USING csv OPTIONS (PATH
                          'src/data/departuredelays.csv')""")

    # GlobalTempView vs TempView: GlobalTempViews can be accessed by any SparkSession  within a Spark application.
    # A TempView can only be accessed by the SparkSession that created it.
    # Accesing spark metadata
    print(spark.catalog.listDatabases())
    print(spark.catalog.listTables())
    print(spark.catalog.listColumns("us_delay_flights_tbl"))

    # DataFrameWriter: access its instance through the dataframe you  want to save
    # ---Parquet

    (flights.write.format("parquet")
     .mode("overwrite")
     .option("compression", "snappy")
     .save("src/data/exports/parquet/flights.parquet"))

    # ---JSON
    (flights.write.format("json")
     .mode("overwrite")
     # .option("compression", "snappy")
     .save("src/data/exports/json/flights.json"))

    # ---CSV
    (flights.write.format("csv")
     .mode("overwrite")
     .save("src/data/exports/csv/flights.csv"))

    # ---AVRO
    '''
    (flights.write.format("avro")
     .mode("overwrite")
     .save("src/data/exports/avro/flights.avro"))
'''
    # ---ORC
    (flights.write.format("orc")
     .mode("overwrite")
     .save("src/data/exports/orc/flights.orc"))

    # DataFrameReader: access through a SparkSession instance
    # ---Parquet
    parquet_read_flights = spark.read.format("parquet").load("src/data/exports/parquet/flights.parquet/")
    parquet_read_flights.show(5)
    # ---JSON
    json_read_flights = spark.read.format("json").load("src/data/exports/json/flights.json")
    json_read_flights.show(5)
    # ---CSV
    csv_read_flights = spark.read.format("csv").load("src/data/exports/csv/flights.csv")
    csv_read_flights.show(5)
    # ---AVRO
    #avro_read_flights = spark.read.format("avro").load("src/data/exports/avro/flights.avro")
    #avro_read_flights.show(5)
    # ---ORC
    orc_read_flights = spark.read.format("orc").load("src/data/exports/orc/flights.orc")
    orc_read_flights.show(5)
    # ---Images
    imagesDF = spark.read.format("image").load("../../../data/train_images/")
    imagesDF.printSchema()
    imagesDF.select("image.height", "image.width", "image.nChannels", "image.mode", "label").show(5, False)
    # ---BinaryFile
    binary_read_flights = (spark.read.format("binaryFile")
                           .option("pathGlobalFilter", "*.jpg")
                           .option("recursiveFileLookup", "true")
                           .load("../../../data/train_images/"))
    binary_read_flights.show(5)

    # Stop the SparkSession
    spark.stop()
