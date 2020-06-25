package com.comp.etl.mock.database.model

import java.time.temporal.ChronoUnit
import java.util.Date

import com.github.javafaker.Faker
import org.apache.spark.sql.types._

case class Order(
    orderId: Long,
    prodId: Long,
    customerId: Long,
    prodQuotePrice: Double,
    isLeadSale: Boolean,
    crmLeadId: Long,
    companyId: Long,
    createdDate: Date,
) {
  override def toString: String =
    Vector(
      orderId,
      prodId,
      customerId,
      prodQuotePrice,
      isLeadSale,
      crmLeadId,
      companyId,
      createdDate.toInstant.truncatedTo(ChronoUnit.SECONDS).toString,
    ).mkString(",")
}

object Order {

  val INPUT_SCHEMA: StructType = StructType(
    Vector(
      StructField("order_id", LongType),
      StructField("prod_id", LongType),
      StructField("customer_id", LongType),
      StructField("prod_quote_price", FloatType),
      StructField("is_lead_sale", BooleanType),
      StructField("crm_lead_Id", LongType),
      StructField("company_id", LongType),
      StructField("created_date", TimestampType),
    ),
  )
  def mock(
      faker: Faker,
      id: Long,
      prodIds: Long,
      customerId: Long,
      companyId: Long,
      createdDate: Date,
  ): Order = {
    val isSale = faker.bool().bool()
    val price = faker.commerce().price(200.00, 10000.00).toDouble
    val leadId = if (isSale) id else -1
    Order(
      id,
      prodIds,
      customerId,
      price,
      isSale,
      leadId,
      companyId,
      createdDate,
    )
  }
}
