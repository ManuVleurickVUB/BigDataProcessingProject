package project

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

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

    val data = spark.read.json(args(0))
    data.printSchema() // events(array of element(struct of aid(long),ts(long),type(string))) and session(long)
    println("Dataset loaded")


    
    spark.stop()
    println("Spark Session stopped. Program finished.\n")
    
  }
}
