package com.comp

import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {
  lazy val spark: SparkSession = {
    val spk = SparkSession
      .builder()
      .master("local")
      .appName("spark test example")
      .getOrCreate()
    spk.sparkContext.setLogLevel("ERROR")
    spk
  }
}
