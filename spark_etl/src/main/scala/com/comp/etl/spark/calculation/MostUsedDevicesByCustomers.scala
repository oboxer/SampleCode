package com.comp.etl.spark.calculation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count}

object MostUsedDevicesByCustomers {
  def cal(weblogsDF: DataFrame): DataFrame =
    weblogsDF
      .groupBy(col("device"))
      .agg(count("*").as("total_devcies"))
      .orderBy(col("total_devcies").desc_nulls_last)
      .limit(5)
}
