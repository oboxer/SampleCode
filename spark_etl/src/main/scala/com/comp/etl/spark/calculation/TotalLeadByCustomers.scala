package com.comp.etl.spark.calculation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count}

object TotalLeadByCustomers {
  def cal(ordersDF: DataFrame): DataFrame =
    ordersDF
      .filter(col("is_lead_sale") === true and col("crm_lead_Id") =!= -1)
      .groupBy(col("customer_id"))
      .agg(count("*").as("total_leads"))
}
