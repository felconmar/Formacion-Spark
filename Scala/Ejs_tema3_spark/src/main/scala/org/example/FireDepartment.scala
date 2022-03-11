package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object FireDepartment {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder.master("local[1]")
      .appName("MnMCount")
      .config("spark.sql.debug.maxToStringFields", "200")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

    val fd_path = "src/data/sf-fire-calls.csv"

    // Define schema
    val fire_schema = StructType(Array(StructField("CallNumber", IntegerType, true),
    StructField("UnitID", StringType, true),
    StructField("IncidentNumber", IntegerType, true),
    StructField("CallType", StringType, true),
    StructField("CallDate", StringType, true),
    StructField("WatchDate", StringType, true),
    StructField("CallFinalDisposition", StringType, true),
    StructField("AvailableDtTm", StringType, true),
    StructField("Address", StringType, true),
    StructField("City", StringType, true),
    StructField("Zipcode", IntegerType, true),
    StructField("Battalion", StringType, true),
    StructField("StationArea", StringType, true),
    StructField("Box", StringType, true),
    StructField("OriginalPriority", StringType, true),
    StructField("Priority", StringType, true),
    StructField("FinalPriority", IntegerType, true),
    StructField("ALSUnit", BooleanType, true),
    StructField("CallTypeGroup", StringType, true),
    StructField("NumAlarms", IntegerType, true),
    StructField("UnitType", StringType, true),
    StructField("UnitSequenceInCallDispatch", IntegerType, true),
    StructField("FirePreventionDistrict", StringType, true),
    StructField("SupervisorDistrict", StringType, true),
    StructField("Neighborhood", StringType, true),
    StructField("Location", StringType, true),
    StructField("RowID", StringType, true),
    StructField("Delay", FloatType, true)));
    // Read csv
    val fire_df = spark.read.schema(fire_schema)
      .option("header", "true")
      .csv(fd_path);
    // Selection from dataframe of "Medical Incident"
    val fewFireDF = fire_df.select("IncidentNumber", "AvailableDtTm", "CallType").where(col("CallType").isNotNull);
    fewFireDF.show(10, false);

    // Distinct function
    val distinct_fire_df = fire_df
        .select("CallType")
        .where(col("CallType").isNotNull)
        .agg(countDistinct("CallType") as 'DistinctCallTypes);

    distinct_fire_df.show(10, false)
    // Column renaming
    val renamed_fire_df = fire_df.withColumnRenamed("Delay", "ResponseDelayedInMins");
    renamed_fire_df
      .select("ResponseDelayedInMins")
      .where(renamed_fire_df("ResponseDelayedInMins") > 5)
      .show(5, false);
    // Changing to timestamp
    val timestamp_fire_df = renamed_fire_df
      .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
      .drop(col("CallDate"))
      .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
      .drop("WatchDate")
      .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a"))
      .drop("AvailableDtTm");
    (timestamp_fire_df.select("IncidentDate", "OnWatchDate", "AvailableDtTS").show(5, false))
    // Years of call
    timestamp_fire_df.select(year(col("IncidentDate"))).distinct().orderBy(year(col("IncidentDate"))).show();
    // Most common type of fire calls
    println("Most common type of fire calls")
    (timestamp_fire_df.groupBy("CallType").count().orderBy(desc("count")).show(truncate=false))

    (timestamp_fire_df.select("CallType").groupBy("CallType").agg(count("CallType").alias("Cantidad")).orderBy(
      desc("Cantidad")).show(truncate=false))

    // Book solution
      (timestamp_fire_df
        .select("CallType")
        .where(col("CallType").isNotNull)
        .groupBy("CallType")
        .count()
        .orderBy(desc("count"))
        .show(truncate=false))
    /*
    Compute the sum of alarms, the average response time, and the minimum
    and maximum response times to all fire calls in our data set, importing the PySpark
    functions in a Pythonic way so as not to conflict with the built-in Python functions
    */
    println("Compute the sum of alarms, the average response time, and the minimum\n " +
      "   and maximum response times to all fire calls in our data set, importing the PySpark\n " +
      "   functions in a Pythonic way so as not to conflict with the built-in Python functions")
    (timestamp_fire_df
      .select(sum("NumAlarms"), avg("ResponseDelayedinMins"),
        min("ResponseDelayedinMins"), max("ResponseDelayedinMins"))
      .show())

    // What were all the different types of fire calls in 2018?
    println("What were all the different types of fire calls in 2018?")
      (timestamp_fire_df.where(year(col("IncidentDate")) === 2018).select(col("CallType")).distinct().show(truncate=false))
    // What months within the year 2018 saw the highest number of fire calls?
    println("What months within the year 2018 saw the highest number of fire calls?")
    (timestamp_fire_df.where(year(col("IncidentDate")) === 2018).groupBy(month(col("IncidentDate")))
      .agg(count("IncidentNumber").alias("Cantidad")).orderBy(desc("Cantidad")).show(truncate=false))
    //-------------------------------------
    print("---------------------------------------")
    (timestamp_fire_df.where(year(col("IncidentDate")) === 2018).groupBy(month(col("IncidentDate")))
      .count().orderBy(desc("count")).show(truncate=false))
    // Which neighborhood in San Francisco generated the most fire calls in 2018?
    println("Which neighborhood in San Francisco generated the most fire calls in 2018?")
      (timestamp_fire_df.where(year(col("IncidentDate")) === 2018).groupBy(col("Neighborhood"))
        .count().orderBy(desc("count")).show(truncate=false))
    // Which neighborhoods had the worst response times to fire calls in 2018?
    println("Which neighborhoods had the worst response times to fire calls in 2018?")
      (timestamp_fire_df.where(year(col("IncidentDate")) === 2018).groupBy(col("Neighborhood"))
        .agg(sum("ResponseDelayedInMins").alias("Suma_ResponseTime"),
          avg("ResponseDelayedInMins").alias("Avg_ResponseTime")).orderBy(desc("Suma_ResponseTime")).show(
        truncate=false))
    // Which week in the year in 2018 had the most fire calls?
    println("Which week in the year in 2018 had the most fire calls?")
    (timestamp_fire_df.where(year(col("IncidentDate")) === 2018).groupBy(weekofyear(col("IncidentDate")))
      .count().orderBy(desc("count")).show(truncate=false))
    // Is there a correlation between neighborhood, zip code, and number of fire calls?
    println("Is there a correlation between neighborhood, zip code, and number of fire calls?")
    (timestamp_fire_df.groupBy(col("Neighborhood"), col("Zipcode"))
      .agg(count(col("CallNumber")).alias("Cantidad_llamadas")).orderBy(desc("Cantidad_llamadas")).show(truncate=false))

    // Stop the SparkSession
    spark.stop()
  }
}
