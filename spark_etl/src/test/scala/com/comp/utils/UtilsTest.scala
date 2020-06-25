package com.comp.utils

import java.util.Random

import com.comp.etl.mock.database.model.DataBases
import com.comp.utils.Utils.MinMaxRange
import com.github.javafaker.Faker
import org.joda.time.{DateTime, DateTimeZone}
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

class UtilsTest extends AsyncFreeSpec with Matchers {

  "Utils" - {
    "mockData" - {
      "init data generation with min=1 and max=5000" - {
        val random = new Random(0)
        val faker = Faker.instance(random)
        val toDate = new DateTime(2021, 6, 1, 0, 0).withZone(DateTimeZone.UTC).toDate

        "company" - {
          "id should start from 0" in {
            val actual = Utils.mockData(0, DataBases.Companies, faker, random, toDate, null, true)
            assert("0,Ankunding Inc" == actual)
          }
        }

        "customer" - {
          "id should start from 1" in {
            val actual = Utils.mockData(1, DataBases.Customers, faker, random, toDate, null, true)
            val expected = "1"
            assert(actual.startsWith(expected))
          }
        }

        "order" - {
          "id should start from 1" in {
            val actual = Utils.mockData(1, DataBases.Orders, faker, random, toDate, null, true)
            val expected = "1,2656,864"
            assert(actual.startsWith(expected))
          }
        }

        "csv" - {
          "CompanyCsv should be empty" in {
            val actual = Utils.mockData(1, DataBases.CompanyCsv, faker, random, toDate, null, true)
            assert(actual.isEmpty)
          }
          "LeadCsv should be empty" in {
            val actual = Utils.mockData(1, DataBases.LeadCsv, faker, random, toDate, null, true)
            assert(actual.isEmpty)
          }
        }
      }

      "daily date generation" - {
        val minMaxRange = MinMaxRange(
          maxProdId = 123,
          maxOrderId = 465,
          maxCompanyId = 1000,
          maxCustomerId = 756,
          maxLeadId = 345,
        )
        val random = new Random(5)
        val faker = Faker.instance(random)
        val toDate = new DateTime(2021, 6, 1, 0, 0).withZone(DateTimeZone.UTC).toDate

        "company" - {
          "id should start from 3000" ignore {
            val actual = Utils.mockData(2000, DataBases.Companies, faker, random, toDate, minMaxRange)
            assert(actual == "3000,Feil Aufderhar and Murazik")
          }
        }

        "customer" - {
          "id should start from 757" ignore {
            val actual = Utils.mockData(1, DataBases.Customers, faker, random, toDate, minMaxRange)
            val expected =
              "757,Mr. Blossom Bogisich,1985-03-22T00:00:00Z,andreas.barrows,andy.lemke@example.com,London,arg"
            assert(actual == expected)
          }
        }

        "order" - {
          "should be empty" ignore {
            val actual = Utils.mockData(1, DataBases.Orders, faker, random, toDate, minMaxRange)
            assert(actual.isEmpty)
          }
        }

        "csv" - {
          "CompanyCsv should have results" in {
            val actual = Utils.mockData(1, DataBases.CompanyCsv, faker, random, toDate, minMaxRange)
            assert(!actual.isEmpty)
          }
          "LeadCsv should have results" in {
            val actual = Utils.mockData(1, DataBases.LeadCsv, faker, random, toDate, minMaxRange)
            assert(!actual.isEmpty)
          }
        }
      }
    }
  }
}
