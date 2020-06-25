package com.comp.etl.spark.transform

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame}

object CompanyCatalog {
  val outputCols: Vector[Column] = Vector(
    col("prod_id"),
    col("company_id"),
    col("supplier_id"),
    col("prod_unit_price"),
  )

  def trans(orders: DataFrame, products: DataFrame): DataFrame = {
    val ordersDF = orders
      .select(col("prod_id"), col("company_id"))
      .distinct()

    val productsLimited = products.select(col("prod_id"), col("supplier_id"), col("prod_unit_price"))

    ordersDF.join(productsLimited, "prod_id")
  }
}
