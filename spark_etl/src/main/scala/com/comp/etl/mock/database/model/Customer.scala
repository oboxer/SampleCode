package com.comp.etl.mock.database.model

import java.time.temporal.ChronoUnit
import java.util.Date

import com.github.javafaker.Faker
import org.apache.spark.sql.types._

case class Customer(
    customerId: Long,
    fullName: String,
    dob: Date,
    userName: String,
    email: String,
    mostFreqCity: String,
    mostFreqCntry: String,
) {
  private val dayStr: String = dob.toInstant.truncatedTo(ChronoUnit.DAYS).toString
  override def toString: String =
    Vector(customerId, fullName, dayStr, userName, email, mostFreqCity, mostFreqCntry).mkString(",")
}

object Customer {
  val INPUT_SCHEMA: StructType = StructType(
    Vector(
      StructField("customer_id", LongType),
      StructField("full_name", StringType),
      StructField("dob", TimestampType),
      StructField("user_name", StringType),
      StructField("email", StringType),
      StructField("most_freq_city", StringType),
      StructField("most_freq_cntry", StringType),
    ),
  )
  def mock(faker: Faker, id: Long): Customer = {
    val country = faker.country()
    Customer(
      id,
      faker.name().fullName(),
      faker.date().birthday(18, 99),
      faker.name().username(),
      faker.internet().safeEmailAddress(),
      country.capital(),
      country.countryCode3(),
    )
  }
}
