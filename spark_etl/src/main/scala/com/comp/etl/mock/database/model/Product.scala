package com.comp.etl.mock.database.model

import com.github.javafaker.Faker
import org.apache.spark.sql.types._

case class Product(prodId: Long, prodName: String, prodUnitPrice: Currency, supplierId: Long) {
  override def toString: String =
    Vector(prodId, prodName, prodUnitPrice.price, prodUnitPrice.currencyCode, supplierId).mkString(",")
}

object Product {
  val INPUT_SCHEMA: StructType = StructType(
    Vector(
      StructField("prod_id", LongType),
      StructField("prod_name", StringType),
      StructField("prod_unit_price", FloatType),
      StructField("prod_currency_code", StringType),
      StructField("supplier_id", LongType),
    ),
  )
  def mock(faker: Faker, id: Long, supplierId: Long): Product =
    Product(
      id,
      faker.commerce().productName(),
      Currency(faker.commerce().price(10.00, 10000.00).toDouble),
      supplierId,
    )
}
