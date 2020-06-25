package com.comp.etl.spark.calculation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object MostPopularProducts {
  def cal(weblogsDF: DataFrame, ordersDF: DataFrame, customersDF: DataFrame): DataFrame =
    weblogsDF.transform(withOrdeewr(_, ordersDF, customersDF))

//  private def withTopCountryWithMostLoggedInUsers(weblogsDF: DataFrame): DataFrame = {
//    weblogsDF.groupBy(col("country")).agg(countDistinct("user_name").as("total_distinct_users"))
//      .orderBy(col("total_distinct_users").desc_nulls_last).limit(1)
//  }

  private def withOrdeewr(weblogsDF: DataFrame, ordersDF: DataFrame, customersDF: DataFrame): DataFrame = {
    val topCountryWMostUsers = weblogsDF
      .groupBy(col("country"))
      .agg(countDistinct("user_name").as("total_distinct_users"))
      .orderBy(col("total_distinct_users").desc_nulls_last)
      .limit(1)
      .collect()
      .head
      .get(0)
      .toString

    val topCuntryWUsers = weblogsDF.filter(col("country") === topCountryWMostUsers).select("user_name", "country")

    ordersDF
      .select("prod_id", "customer_id")
      .join(customersDF.select("customer_id", "user_name"), "customer_id")
      .distinct()
      .join(topCuntryWUsers, "user_name")
      .drop("user_name", "customer_id")
      .distinct()
  }
}
