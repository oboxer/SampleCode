package com.comp.etl.spark.calculation

import com.comp.etl.spark.transform.CompanyCatalog
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object MonthlySales {
  def cal(ordersDF: DataFrame, productsDF: DataFrame): DataFrame = {
    val companyCatalogsDF = CompanyCatalog.trans(ordersDF, productsDF)

    ordersDF
      .transform(withCalSales)
      .transform(withProductUnitPrice(_, companyCatalogsDF))
      .transform(withTotalProfit)
      .orderBy(col("monthly_profit").desc_nulls_last)
  }

  private def withCalSales(ordersDF: DataFrame): DataFrame =
    ordersDF
      .withColumn("sale_month", month(col("created_date")))
      .withColumn("sale_year", year(col("created_date")))
      .groupBy(col("company_id"), col("prod_id"), col("sale_month"), col("sale_year"))
      .agg(sum("prod_quote_price").as("total_sales"), count("*").as("total_units_sold"))

  private def withProductUnitPrice(salesDF: DataFrame, companyCatalogsDF: DataFrame): DataFrame =
    salesDF.join(companyCatalogsDF, Seq("company_id", "prod_id"))

  private def withTotalProfit(salesCatalogsDF: DataFrame): DataFrame = {
    val profit = col("total_sales") - (col("prod_unit_price") * col("total_units_sold"))

    val totalProfit = round(sum("total_profit"), 2).as("monthly_profit")
    val totalSales = round(sum("total_sales"), 2).as("monthly_sales")

    salesCatalogsDF
      .withColumn("total_profit", profit)
      .groupBy(col("sale_month"), col("sale_year"))
      .agg(totalSales, totalProfit)
  }
}
