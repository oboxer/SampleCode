package com.comp.etl.spark

import java.util.{Date, TimeZone}

import com.comp.etl.mock.database.model._
import com.comp.etl.spark.calculation._
import com.comp.etl.spark.out.ParquetOut
import com.comp.etl.spark.transform.{CombineOrdersEtl, WeblogEtl}
import com.comp.utils.ArgsHelper
import com.comp.utils.ArgsHelper.AddOptions
import org.apache.spark.sql.SparkSession
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}

object MetricsEtl {

  final private case class Params(
      dateStr: String,
      date: Date,
      yesterdayDateStr: String,
      yesterdayDate: Date,
      s3Key: String,
      s3Sec: String,
      isInitLoad: Boolean,
  )

  def main(args: Array[String]): Unit = {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

    val params = setUpParams(args)
    val spark = setUpSpark(params)

    val date = params.dateStr

    val allDFs = if (params.isInitLoad) initLoad(spark) else dailyRefresh(params, spark)

    // write daily new snapshot to S3
    allDFs
      .getPaths(date)
      .filterNot(_._2.isEmpty)
      .foreach(path_df => ParquetOut.fileOut(path_df._2, path_df._1))

    if (!params.isInitLoad) {
      // calculate metrics and save to S3
      Vector(
        (s"report=monthly_sales/$date", MonthlySales.cal(allDFs.updatedOrdersDF, allDFs.productDF)),
        (
          s"report=most_popular_products/$date",
          MostPopularProducts.cal(allDFs.parsedWebLogDF, allDFs.updatedOrdersDF, allDFs.customerDF),
        ),
        (s"report=most_used_devices/$date", MostUsedDevicesByCustomers.cal(allDFs.parsedWebLogDF)),
        (s"report=total_leads/$date", TotalLeadByCustomers.cal(allDFs.updatedOrdersDF)),
      ).foreach(path_df => ParquetOut.fileOut(path_df._2, path_df._1))
    }
  }

  private def setUpParams(args: Array[String]): Params = {
    val isInitOption = AddOptions("i", "init", "Is this init data load. EG: False")
    val dateOption = AddOptions("d", "date", "Process date; EG: 2020-06-20")
    val s3KeyOption = AddOptions("s3k", "s3key", "S3 key")
    val s3SecOption = AddOptions("s3s", "s3sec", "S3 secret")

    val map = ArgsHelper.createArgs(
      args,
      Vector(
        isInitOption,
        dateOption,
        s3KeyOption,
        s3SecOption,
      ),
    )

    val formatter = DateTimeFormat.forPattern("yyyy-MM-dd").withZone(DateTimeZone.UTC)

    val isInit = map(isInitOption.symbol).toBoolean
    val dateStr = map(dateOption.symbol)
    val date = formatter.parseDateTime(dateStr).toDate
    val ydate = new DateTime(date.toInstant.toEpochMilli).minusDays(1)
    val yesterdayDate = ydate.toDate
    val yesterdayDateStr = formatter.print(ydate)
    val s3Key = map(s3KeyOption.symbol)
    val s3Sec = map(s3SecOption.symbol)

    Params(dateStr, date, yesterdayDateStr, yesterdayDate, s3Key, s3Sec, isInit)
  }

  private def setUpSpark(params: Params): SparkSession = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("spark preproces mock data")
      .getOrCreate()

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", params.s3Key)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", params.s3Sec)

    spark
  }

  private def initLoad(spark: SparkSession): PreprocessDFs = {
    val s3Path = "s3a://com.comp.prod.data.etl/data/init/"

    val companyDF =
      spark.read.schema(Company.INPUT_SCHEMA).csv(s3Path + DataBases.Companies.toString.toLowerCase + ".csv")
    val customerDF =
      spark.read.schema(Customer.INPUT_SCHEMA).csv(s3Path + DataBases.Customers.toString.toLowerCase + ".csv")
    val orderDF = spark.read.schema(Order.INPUT_SCHEMA).csv(s3Path + DataBases.Orders.toString.toLowerCase + ".csv")
    val productDF =
      spark.read.schema(Product.INPUT_SCHEMA).csv(s3Path + DataBases.Products.toString.toLowerCase + ".csv")

    PreprocessDFs(
      companyDF,
      customerDF,
      orderDF,
      productDF,
      spark.emptyDataFrame,
    )
  }

  private def dailyRefresh(params: Params, spark: SparkSession): PreprocessDFs = {
    val s3RawDataPath = "s3a://com.comp.prod.data.etl/data/raw/" + params.dateStr
    val s3SnapShotpath = "s3a://com.comp.prod.data.etl/data/final/table="

    val companyDF = spark.read.parquet(s3SnapShotpath + "companies/" + params.yesterdayDateStr)
    val customerDF = spark.read.parquet(s3SnapShotpath + "customers/" + params.yesterdayDateStr)
    val orderDF = spark.read.parquet(s3SnapShotpath + "orders/" + params.yesterdayDateStr)
    val productDF =
      spark.read.parquet(s3SnapShotpath + "products/" + params.yesterdayDateStr)

    val parsedWebLogDF = WeblogEtl.trans(spark.read.option("mode", "DROPMALFORMED").text(s"$s3RawDataPath/weblogs.log"))

    val updatedOrdersDF = CombineOrdersEtl.trans(spark, s3RawDataPath, orderDF)

    PreprocessDFs(
      companyDF.persist(),
      customerDF.persist(),
      updatedOrdersDF.persist(),
      productDF.persist(),
      parsedWebLogDF.persist(),
    )
  }
}
