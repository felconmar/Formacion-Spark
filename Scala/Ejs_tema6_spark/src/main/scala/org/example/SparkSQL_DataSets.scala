package org.example

import com.databricks.spark.xml.XmlDataFrameReader
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat, concat_ws, sha2}

import scala.util.Random._

case class Usage(uid:Int, uname:String, usage: Int)
case class UsageCost(uid: Int, uname:String, usage: Int, cost: Double)
case class Zip(city: String, loc: Array[Double], pop: Long, state: String, _id: String)

object SparkSQL_DataSets {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder.master("local[1]")
      .appName("DataSets")
      .config("spark.sql.debug.maxToStringFields", "200")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    import spark.implicits._

    val r = new scala.util.Random(42)
    // Create 1000 instances of scala Usage class
    // This generates data on the fly
    val data = for (i <- 0 to 1000)
      yield (Usage(i, "user-" + r.alphanumeric.take(5).mkString(""),
        r.nextInt(1000)))
    // Create a Dataset of Usage typed data
    val dsUsage = spark.createDataset(data)
    //dsUsage.show(10)

    //(dsUsage.filter(o => o.usage > 900).show(10))

    def computeUserCostUsage(u: Usage): UsageCost = {
      val v = if (u.usage > 700) u.usage * 0.15 else u.usage * 0.50
      UsageCost(u.uid, u.uname, u.usage, v)
    }
    //dsUsage.map(u => {computeUserCostUsage(u)}).show(10)

    val zips = spark.read
      .format("json")
      .option("path", "src/data/zips.json")
      .load()
      .as[Zip]
    //zips.show(10)


    val people_xml = spark.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "person")
      .xml("src/data/people.xml")
      .withColumn("HASH",sha2(concat_ws("/", col("id"), col("name"),
        col("lastname"), col("sex"), col("weight"), col("name"),
        col("age"), col("height")), 0))
    people_xml.show()


    val people_csv = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("sep", ";")
      .csv("src/data/people.csv")
      .withColumn("HASH",sha2(concat_ws("/", col("id"), col("name"),
        col("lastname"), col("sex"), col("weight"), col("name"),
        col("age"), col("height")), 0))
    people_csv.show()

    //|age|height| id|       lastname|        name|   sex|weight|

    val diff_xml_csv = people_xml.join(people_csv, Seq("HASH"), "right_outer")
    diff_xml_csv.show(false)





    // Stop the SparkSession
    spark.stop()
  }
}
