package com.examples

import org.apache.spark.sql.SparkSession

object SparkPhoenixExample {

  def main(args: Array[String]): Unit = {
    // Create Spark session
    val spark: SparkSession = SparkSession.builder()
      .appName("Phoenix Spark Integration Example")
      .getOrCreate()

    // Configure Phoenix JDBC URL
    val phoenixJdbcUrl = "jdbc:phoenix:master-01.tdp:2181:/hbase" // Replace with your Zookeeper's host and port

    // Define the table name in Phoenix
    val tableName = "TEST_TABLE"

    // Load data from Phoenix table using DataFrame
    val df = spark.read
      .format("org.apache.phoenix.spark")
      .option("table", tableName)
      .option("zkUrl", phoenixJdbcUrl)
      .load()

    // Show the data from DataFrame
    df.show()

    // Stop the Spark session
    spark.stop()
  }
}
