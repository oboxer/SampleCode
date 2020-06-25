package com.comp.etl.mock.database.csv

import java.time.temporal.ChronoUnit
import java.util.Date

import com.github.javafaker.Faker

case class LeadCsv(
    leadId: Long,
    quoteProductId: Long,
    quotePrice: Double,
    quoteValue: Double,
    saleFlag: Boolean,
    orderId: Long,
    customerId: Long,
    companyId: Long,
    createdDate: Date,
) {
  private val orderIdStr = if (orderId == -1) "" else orderId.toString

  override def toString: String =
    Vector(
      leadId,
      quoteProductId,
      quotePrice,
      quoteValue,
      saleFlag,
      orderIdStr,
      customerId,
      companyId,
      createdDate.toInstant.truncatedTo(ChronoUnit.SECONDS).toString,
    ).mkString(",")
}

object LeadCsv {
  def mock(
      faker: Faker,
      id: Long,
      prodIds: Long,
      maxOrderId: Long,
      customerId: Long,
      companyId: Long,
      date: Date,
  ): LeadCsv = {
    val isSale = faker.bool().bool()
    val price = faker.commerce().price(200.00, 10000.00).toDouble
    val value = faker.commerce().price(price * 1.5, price * 5.0).toDouble
    val orderId = if (isSale) maxOrderId + id else -1
    LeadCsv(
      id,
      prodIds,
      price,
      value,
      isSale,
      orderId,
      customerId,
      companyId,
      date,
    )
  }
}
