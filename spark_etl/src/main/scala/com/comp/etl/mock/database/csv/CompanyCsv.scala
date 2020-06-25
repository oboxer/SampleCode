package com.comp.etl.mock.database.csv

import java.time.temporal.ChronoUnit
import java.util.Date

import com.github.javafaker.Faker

case class CompanyCsv(
    quoteProductId: Long,
    quotePrice: Double,
    quoteValue: Double,
    orderId: Long,
    customerId: Long,
    companyId: Long,
    supplierId: Long,
    createdDate: Date,
) {
  private val orderIdStr = if (orderId == -1) "" else orderId.toString

  override def toString: String =
    Vector(
      quoteProductId,
      quotePrice,
      quoteValue,
      orderIdStr,
      customerId,
      companyId,
      supplierId,
      createdDate.toInstant.truncatedTo(ChronoUnit.SECONDS).toString,
    ).mkString(",")
}

object CompanyCsv {
  def mock(
      faker: Faker,
      prodId: Long,
      orderId: Long,
      customerId: Long,
      companyId: Long,
      supplierId: Long,
      date: Date,
  ): CompanyCsv = {
    val price = faker.commerce().price(10.00, 10000.00).toDouble
    val value = faker.commerce().price(price * 1.5, price * 5.0).toDouble
    CompanyCsv(
      prodId,
      price,
      value,
      orderId,
      customerId,
      companyId,
      supplierId,
      date,
    )
  }
}
