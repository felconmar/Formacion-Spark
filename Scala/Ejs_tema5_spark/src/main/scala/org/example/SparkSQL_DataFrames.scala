package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object SparkSQL_DataFrames {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder.master("local[1]")
      .appName("Flights")
      .config("spark.sql.debug.maxToStringFields", "200")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

    //Cubed function
    //importante, comprobar que no es nulo aquí ya que no se garantiza el orden del null checking en la clausula where
    //            es por esto que lo más seguro es realizar una comprobación en la función o usar case
    val cubed = (s: Long) => {
      if (s != null) {
        s * s * s
      }
    }
    //Register UDF
    //Las UDFs
    spark.udf.register("cubed", cubed)

    //Create a temporary view
    spark.range(1, 9).createOrReplaceTempView("udf_test")

    spark.sql("select id, cubed(id) as id_cubed from udf_test").show()


    // Stop the SparkSession
    spark.stop()
  }
}
