package org.example

import com.databricks.spark.xml.XmlDataFrameReader
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat, concat_ws, expr, sha2, udf, when}

import scala.util.Random._

object Compare_xml_csv {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder.master("local[1]")
      .appName("DataSets")
      .config("spark.sql.debug.maxToStringFields", "200")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

    val people_xml = spark.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "person")
      .xml("src/data/people.xml")
      .withColumn("HASH",sha2(concat_ws("/", col("name"),
        col("lastname"), col("sex"), col("weight"), col("age"),
        col("height")), 0))
    //people_xml.show()


    val people_csv = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("sep", ";")
      .csv("src/data/people.csv")
      .withColumn("HASH",sha2(concat_ws("/", col("name"),
        col("lastname"), col("sex"), col("weight"), col("age"),
        col("height")), 0))
    people_csv.show()

    //|age|height| id|       lastname|        name|   sex|weight|

    //val inner_xml_csv = people_xml.join(people_csv, Seq("HASH"), "inner")
    //inner_xml_csv.show(false)

    val diff_xml_csv = people_xml.join(people_csv, people_csv("id") === people_xml("id"), "outer")
      .where(people_csv("name").isNull or
        people_xml("name").isNull or
        people_csv("HASH") =!= people_xml("HASH"))
      .withColumn("DIFF",
        when(people_csv("name").isNull, "NEW_FROM_XML")
          .when(people_xml("name").isNull, "NEW_FROM_CSV")
          .when(people_csv("HASH") =!= people_xml("HASH"), "TRUE"))
      .withColumn("_ID_",
        when(people_csv("id").isNotNull,people_csv("id"))
          .otherwise(people_xml("id")))
      //Sería más correcto que pusiera en vez de new, only. Como ha dicho monte, puede que se borre de un sitio
      //Es por esto que parece más correcto el nombre de only al nombre de new.
    diff_xml_csv
      .select(col("_ID_") as "ID", col("DIFF"))
      .orderBy(col("ID"))
      .show(false)

    //-----------------------------------------------------------
    val comp: (Int => String) = (arg: Int) => {if (arg < 100) "aaaaaaaaaaaaaaaaaaaaa" else "a"}
    val col_comparator = udf(comp)

    diff_xml_csv
      .where(col("DIFF") === "TRUE")
      //.withColumn("CHANGES", col_comparator(col("age")))
      //.show(false)
   /*
   val a = "a"
    print(a)
    print(a - a)
    */

    /*
    val new_xml_csv = people_xml.join(people_csv, people_csv("id") === people_xml("id"), "full")
      .withColumn("FROM_DB", when(people_csv("name").isNull, "XML").when(people_xml("name").isNull, "CSV"))
      .withColumn("NEW_ID", when(people_csv("id").isNotNull,people_csv("id")).otherwise(people_xml("id")))
      .select(col("NEW_ID"), col("FROM_DB"))
    new_xml_csv.show(false)
*/




    // Stop the SparkSession
    spark.stop()
  }
}
