package com.comp.etl.spark.transform

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}

object LeadsCsvEtl extends TransformEtl {

  val INPUT_SCHEMA: StructType = StructType(
    Vector(
      StructField("crm_lead_Id", LongType),
      StructField("prod_id", LongType),
      StructField("prod_quote_price", DoubleType),
      StructField("quote_value", DoubleType),
      StructField("is_lead_sale", BooleanType),
      StructField("order_id", LongType),
      StructField("customer_id", LongType),
      StructField("company_id", LongType),
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
    input.filter(col("is_lead_sale") === true and col("order_id").isNotNull).select(outputCols: _*)

}
