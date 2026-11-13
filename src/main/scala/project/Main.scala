package project

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions._

// sbt assembly
// spark-submit --master "local[*]" ./project.jar data/otto-tiny.jsonl

object Main {
  def main(args: Array[String]): Unit = {
    require(
      args.length == 1,
      "Usage: Main <input-jsonl-path>"
    )
    
    val conf = new SparkConf()

    // If no master is set (e.g., via spark-submit), default to local[*]
    if (!conf.contains("spark.master")) {
      conf.setMaster("local[*]")
    }

    val spark = SparkSession.builder
      .appName("workspace")
      .config(conf)
      .getOrCreate();

    val sc = spark.sparkContext

    println(s"Starting Spark job on ${sc.master}:")

    // 3.2
    val dataDF = spark.read.json(args(0))
    dataDF.printSchema() // events(array of element(struct of aid(long),ts(long),type(string))) and session(long)
    println(s"Dataset ${args(0)} loaded")

    // Intermediate representation: Spark DataFrame since easy to use while getting Spark SQL optimizations

    // 3.3
    val events = dataDF.withColumn("event", explode(col("events")))
    events.select(col("event.type")).distinct().show(false) // clicks, orders, carts

    events.groupByKey(col("aid")).max()

    val amountOfClicks = events.filter(col("event.type") === "clicks").count()
    val amountOfOrders = events.filter(col("event.type") === "orders").count()
    val amountOfCarts = events.filter(col("event.type") === "carts").count()
    println(s"Amount of clicks: ${amountOfClicks}")
    println(s"Amount of orders: ${amountOfOrders}")
    println(s"Amount of carts: ${amountOfCarts}")


    
    spark.stop()
    println("Spark Session stopped. Program finished.\n")
    
  }
}
