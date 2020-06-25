package com.comp.etl.mock.database.model

import com.github.javafaker.Faker
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

case class Company(companyId: Long, companyName: String) {
  override def toString: String = Vector(companyId, companyName.replaceAll(",", "")).mkString(",")
}

object Company {

  val INPUT_SCHEMA: StructType = StructType(
    Vector(
      StructField("company_id", LongType),
      StructField("company_name", StringType),
    ),
  )

  def mock(faker: Faker, id: Long): Company = Company(id, faker.company().name())
}
