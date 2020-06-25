package com.comp.etl.spark.transform

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.types.{DoubleType, LongType, StructField, StructType, TimestampType}

object CompanyCsvEtl extends TransformEtl {

  val INPUT_SCHEMA: StructType = StructType(
    Vector(
      StructField("prod_id", LongType),
      StructField("prod_quote_price", DoubleType),
      StructField("quote_value", DoubleType),
      StructField("order_id", LongType),
      StructField("customer_id", LongType),
      StructField("company_id", LongType),
      StructField("supplier_id", LongType),
      StructField("created_date", TimestampType),
    ),
  )
  override val outputCols: Vector[Column] = Vector(
    col("order_id"),
    col("prod_id"),
    col("customer_id"),
    col("prod_quote_price"),
    col("is_lead_sale"),
    col("crm_lead_Id"),
    col("company_id"),
    col("created_date"),
  )

  override def trans(input: DataFrame): DataFrame =
    input
      .withColumn("is_lead_sale", lit(false))
      .withColumn("crm_lead_Id", lit(-1))
      .select(outputCols: _*)
}
