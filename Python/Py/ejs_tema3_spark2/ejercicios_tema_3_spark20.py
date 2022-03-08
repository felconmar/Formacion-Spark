import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

if __name__ == "__main__":

    fd_path = os.path.realpath(os.path.join(os.path.dirname(__file__), '../../..', 'data', 'sf-fire-calls.csv'))

    # Create SparkSession
    spark = (SparkSession
             .builder
             .appName("FireDepartmentSession")
             .config('spark.sql.debug.maxToStringFields', '200')
             .getOrCreate())
    # Define schema
    fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
                              StructField('UnitID', StringType(), True),
                              StructField('IncidentNumber', IntegerType(), True),
                              StructField('CallType', StringType(), True),
                              StructField('CallDate', StringType(), True),
                              StructField('WatchDate', StringType(), True),
                              StructField('CallFinalDisposition', StringType(), True),
                              StructField('AvailableDtTm', StringType(), True),
                              StructField('Address', StringType(), True),
                              StructField('City', StringType(), True),
                              StructField('Zipcode', IntegerType(), True),
                              StructField('Battalion', StringType(), True),
                              StructField('StationArea', StringType(), True),
                              StructField('Box', StringType(), True),
                              StructField('OriginalPriority', StringType(), True),
                              StructField('Priority', StringType(), True),
                              StructField('FinalPriority', IntegerType(), True),
                              StructField('ALSUnit', BooleanType(), True),
                              StructField('CallTypeGroup', StringType(), True),
                              StructField('NumAlarms', IntegerType(), True),
                              StructField('UnitType', StringType(), True),
                              StructField('UnitSequenceInCallDispatch', IntegerType(), True),
                              StructField('FirePreventionDistrict', StringType(), True),
                              StructField('SupervisorDistrict', StringType(), True),
                              StructField('Neighborhood', StringType(), True),
                              StructField('Location', StringType(), True),
                              StructField('RowID', StringType(), True),
                              StructField('Delay', FloatType(), True)])
    # Read csv
    fire_df = spark.read.csv(fd_path, header=True, schema=fire_schema)
    # Export to parquet file system
    fire_parquet_path = os.path.realpath(
        os.path.join(os.path.dirname(__file__), 'exports', 'sf-fire-calls_parquet.parquet'))
    if not os.path.exists(fire_parquet_path):
        fire_df.write.format("parquet").save(fire_parquet_path)
    # Save it as a table, registering metadata with the Hive metastore
    fire_parquet_table = "fire_parquet_table"
    # Ignores the overwrite mode and when i search for the table with a sql command, it can't find it
    # fire_df.write.format("parquet").mode("overwrite").saveAsTable(fire_parquet_table)
    # Selection from dataframe of "Medical Incident"
    medical_fire_df = (fire_df.
                       select("IncidentNumber", "AvailableDtTm", "CallType")
                       .where(F.col("CallType") != "Medical Incident"))
    medical_fire_df.show(5, truncate=False)
    # Distinct function
    distinct_fire_df = (fire_df
                        .select("CallType")
                        .where(F.col("CallType").isNotNull())
                        .distinct())
    distinct_fire_df.show(10, False)
    # Column renaming
    renamed_fire_df = fire_df.withColumnRenamed("Delay", "ResponseDelayedInMins")
    (renamed_fire_df
     .select("ResponseDelayedInMins")
     .where(F.col("ResponseDelayedInMins") > 5)
     .show(5, False))
    # Changing to timestamp
    timestamp_fire_df = (renamed_fire_df
                         .withColumn("IncidentDate", F.to_timestamp(F.col("CallDate"), "MM/dd/yyyy"))
                         .drop("CallDate")
                         .withColumn("OnWatchDate", F.to_timestamp(F.col("WatchDate"), "MM/dd/yyyy"))
                         .drop("WatchDate")
                         .withColumn("AvailableDtTS", F.to_timestamp(F.col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a"))
                         .drop("AvailableDtTm"))
    (timestamp_fire_df.select("IncidentDate", "OnWatchDate", "AvailableDtTS").show(5, False))
    # Years of call
    (timestamp_fire_df.select(F.year("IncidentDate")).distinct().orderBy(F.year("IncidentDate")).show())
    # Most common type of fire calls
    (timestamp_fire_df.groupBy("CallType").count().orderBy("count", ascending=False).show(truncate=False))
    (timestamp_fire_df.select("CallType").groupBy("CallType").agg(F.count("CallType").alias("Cantidad")).orderBy(
        "Cantidad", ascending=False).show(truncate=False))

    # Book solution
    (timestamp_fire_df
     .select("CallType")
     .where(F.col("CallType").isNotNull())
     .groupBy("CallType")
     .count()
     .orderBy("count", ascending=False)
     .show(truncate=False))
    '''
    Compute the sum of alarms, the average response time, and the minimum
    and maximum response times to all fire calls in our data set, importing the PySpark
    functions in a Pythonic way so as not to conflict with the built-in Python functions
    '''
    (timestamp_fire_df
     .select(F.sum("NumAlarms"), F.avg("ResponseDelayedinMins"),
             F.min("ResponseDelayedinMins"), F.max("ResponseDelayedinMins"))
     .show())

    # What were all the different types of fire calls in 2018?
    (timestamp_fire_df.where(F.year("IncidentDate") == 2018).select("CallType").distinct().show(truncate=False))
    # What months within the year 2018 saw the highest number of fire calls?
    (timestamp_fire_df.where(F.year("IncidentDate") == 2018).groupBy(F.month("IncidentDate")) \
     .agg(F.count("IncidentNumber").alias("Cantidad")).orderBy("Cantidad", ascending=False).show(truncate=False))
    #-------------------------------------
    (timestamp_fire_df.where(F.year("IncidentDate") == 2018).groupBy(F.month("IncidentDate")) \
     .count().orderBy("count", ascending=False).show(truncate=False))
    # Which neighborhood in San Francisco generated the most fire calls in 2018?
    (timestamp_fire_df.where(F.year("IncidentDate") == 2018).groupBy(F.col("Neighborhood")) \
     .count().orderBy("count", ascending=False).show(truncate=False))
    # Which neighborhoods had the worst response times to fire calls in 2018?
    (timestamp_fire_df.where(F.year("IncidentDate") == 2018).groupBy(F.col("Neighborhood")) \
     .agg(F.sum("ResponseDelayedInMins").alias("Suma_ResponseTime"),
          F.avg("ResponseDelayedInMins").alias("Avg_ResponseTime")).orderBy("Suma_ResponseTime", ascending=False).show(
        truncate=False))
    # Which week in the year in 2018 had the most fire calls?
    (timestamp_fire_df.where(F.year("IncidentDate") == 2018).groupBy(F.weekofyear("IncidentDate")) \
     .count().orderBy("count", ascending=False).show(truncate=False))
    # Is there a correlation between neighborhood, zip code, and number of fire calls?

    # How can we use Parquet files or SQL tables to store this data and read it back?


    '''
    a. Realizar todos los ejercicios propuestos de libro
    b. Leer el CSV del ejemplo del cap2 y obtener la estructura del schema dado por 
        defecto.
    c. Cuando se define un schema al definir un campo por ejemplo StructField('Delay', 
        FloatType(), True) ¿qué significa el último parámetro Boolean? 
        > Si puede ser nulo.
    d. Dataset vs DataFrame (Scala). ¿En qué se diferencian a nivel de código?
    e. Utilizando el mismo ejemplo utilizado en el capítulo para guardar en parquet y 
        guardar los datos en los formatos:
        i. JSON
        ii. CSV (dándole otro nombre para evitar sobrescribir el fichero origen)
        iii. AVRO
    f. Revisar al guardar los ficheros (p.e. json, csv, etc) el número de ficheros 
        creados, revisar su contenido para comprender (constatar) como se guardan.
        i. ¿A qué se debe que hayan más de un fichero?
        ii. ¿Cómo obtener el número de particiones de un DataFrame?
        iii. ¿Qué formas existen para modificar el número de particiones de un 
            DataFrame?
        iv. Llevar a cabo el ejemplo modificando el número de particiones a 1 y 
            revisar de nuevo el/los ficheros guardados. 
    '''


    # Stop the SparkSession
    spark.stop()
