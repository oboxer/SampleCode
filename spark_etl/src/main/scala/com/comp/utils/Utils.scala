package com.comp.utils

import java.sql.Connection
import java.util.{Date, Random}

import com.comp.etl.mock.database.csv._
import com.comp.etl.mock.database.model._
import com.github.javafaker.Faker
import org.joda.time.{DateTime, DateTimeZone}

object Utils {

  private val INIT_ORDER_DATE = new DateTime(2018, 1, 1, 0, 0).withZone(DateTimeZone.UTC).toDate

  case class MinMaxRange(
      minProdId: Long = 0,
      maxProdId: Long = 5000,
      minOrderId: Long = 0,
      maxOrderId: Long = 5000,
      minCompanyId: Long = 0,
      maxCompanyId: Long = 5000,
      minCustomerId: Long = 0,
      maxCustomerId: Long = 5000,
      minLeadId: Long = 0,
      maxLeadId: Long = 5000,
  )

  case class DataSettings(
      rangeStart: Int = 1,
      rangeEnd: Int = 1000,
      seed: Date = DateTime.now(DateTimeZone.UTC).toDate,
      minMax: MinMaxRange,
  )

  def mockData(
      i: Long,
      modelType: DataBases.Value,
      faker: Faker,
      random: Random,
      toDate: Date,
      minMaxRange: MinMaxRange,
      isInit: Boolean = false,
  ): String =
    if (isInit) mockInitData(i, modelType, faker, random, toDate)
    else mockDailyData(i, modelType, faker, random, toDate, minMaxRange)

  private def mockDailyData(
      i: Long,
      modelType: DataBases.Value,
      faker: Faker,
      random: Random,
      toDate: Date,
      minMaxRange: MinMaxRange,
  ): String = {
    val customerIds = random.longs(minMaxRange.minCustomerId, minMaxRange.maxCustomerId).iterator()
    val companyIds = random.longs(minMaxRange.minCompanyId, minMaxRange.maxCompanyId).iterator()
    val productIds = random.longs(minMaxRange.minProdId, minMaxRange.maxProdId).iterator()
    val orderIds = random.longs(minMaxRange.minOrderId, minMaxRange.maxOrderId).iterator()
    val leadIds = random.longs(minMaxRange.minLeadId, minMaxRange.maxLeadId).iterator()

    val value = modelType match {
      case DataBases.CompanyCsv =>
        CompanyCsv.mock(
          faker,
          productIds.next(),
          orderIds.next() * 2 + i,
          customerIds.next(),
          companyIds.next(),
          companyIds.next(),
          faker.date().between(new DateTime(toDate).minusHours(23).toDate, toDate),
        )
      case DataBases.LeadCsv =>
        LeadCsv.mock(
          faker,
          leadIds.next() + i,
          productIds.next(),
          orderIds.next() + i,
          customerIds.next(),
          companyIds.next(),
          faker.date().between(new DateTime(toDate).minusHours(23).toDate, toDate),
        )
      case _ => ""
    }

    value.toString
  }

  private def mockInitData(
      i: Long,
      modelType: DataBases.Value,
      faker: Faker,
      random: Random,
      toDate: Date,
      minMaxRange: MinMaxRange = MinMaxRange(),
  ): String = {
    val prodIds = random.longs(minMaxRange.minProdId, minMaxRange.maxProdId).iterator()
    val customerIds = random.longs(minMaxRange.minCustomerId, minMaxRange.maxCustomerId).iterator()
    val companyIds = random.longs(minMaxRange.minCompanyId, minMaxRange.maxCompanyId).iterator()

    val value = modelType match {
      case DataBases.Companies => Company.mock(faker, i)
      case DataBases.Customers => Customer.mock(faker, i)
      case DataBases.Orders =>
        Order.mock(
          faker,
          i,
          prodIds.next(),
          customerIds.next(),
          companyIds.next(),
          faker.date().between(INIT_ORDER_DATE, toDate),
        )
      case DataBases.Products                       => Product.mock(faker, i, companyIds.next())
      case DataBases.CompanyCsv | DataBases.LeadCsv => ""
      case _                                        => throw new IllegalArgumentException("not the right model type")
    }

    value.toString
  }

  def generateData(isInit: Boolean, modelType: DataBases.Value, settings: DataSettings): Vector[String] = {
    val random = new Random(settings.seed.getTime)
    val faker = Faker.instance(random)

    Range
      .inclusive(settings.rangeStart, settings.rangeEnd)
      .map(mockData(_, modelType, faker, random, settings.seed, settings.minMax, isInit))
      .toVector
  }

  def getMinMaxFromRedshift(conn: Connection): MinMaxRange = {
    val comp = runQuery(conn, "companies", "company_id")
    val cust = runQuery(conn, "customers", "customer_id")
    val prod = runQuery(conn, "products", "prod_id")
    val lead = runQuery(conn, "orders", "crm_lead_Id")
    val order = runQuery(conn, "orders", "order_id")

    MinMaxRange(prod._1.toInt, prod._2.toInt, order._1, order._2, comp._1, comp._2, cust._1, cust._2, lead._1, lead._2)
  }

  private def runQuery(conn: Connection, tbl: String, sqlKey: String): (Long, Long) = {
    val stmt = conn.createStatement()
    val resultSet = stmt.executeQuery(s"select min($sqlKey), max($sqlKey) from $tbl;")
    val res = new Iterator[(Long, Long)] {
      def hasNext: Boolean = resultSet.next()
      def next(): (Long, Long) = (resultSet.getLong(1), resultSet.getLong(2))
    }.toVector.head
    resultSet.close()
    res
  }
}
