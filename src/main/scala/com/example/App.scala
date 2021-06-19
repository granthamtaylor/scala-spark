package com.example

import org.apache.spark.sql.SparkSession

object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {
    println( "Hello World!" )
    println("concat arguments = " + foo(args))

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local[2]")
      .getOrCreate()

    val df = spark.read.option("header",true).csv("data/friends.csv")

    df.createOrReplaceTempView("people")

    val results = spark.sql("SELECT * FROM people LIMIT 5")

    results.show()

    spark.stop()

  }

}
