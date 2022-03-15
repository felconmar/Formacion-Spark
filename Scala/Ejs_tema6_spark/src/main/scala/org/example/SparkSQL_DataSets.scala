package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
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
    dsUsage.show(10)

    (dsUsage.filter(o => o.usage > 900).show(10))

    def computeUserCostUsage(u: Usage): UsageCost = {
      val v = if (u.usage > 700) u.usage * 0.15 else u.usage * 0.50
      UsageCost(u.uid, u.uname, u.usage, v)
    }
    dsUsage.map(u => {computeUserCostUsage(u)}).show(10)

    val zips = spark.read
      .format("json")
      .option("path", "src/data/zips.json")
      .load()
      .as[Zip]
    zips.show(10)
    // Stop the SparkSession
    spark.stop()
  }
}
