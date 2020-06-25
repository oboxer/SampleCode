package com.comp.etl.mock

import java.sql.DriverManager
import java.util.{Date, TimeZone}

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.comp.etl.mock.database.model.DataBases
import com.comp.utils.ArgsHelper
import com.comp.utils.ArgsHelper.AddOptions
import com.comp.utils.Utils.{DataSettings, MinMaxRange, generateData, getMinMaxFromRedshift}
import org.joda.time.DateTimeZone
import org.joda.time.format.DateTimeFormat

object MockDataGeneration {

  final private case class Params(
      dateStr: String,
      date: Date,
      s3Bucket: String,
      s3Path: String,
      s3Client: AmazonS3,
      settings: DataSettings,
      isInitLoad: Boolean,
  )

  def main(args: Array[String]): Unit = {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

    val params = setUpParams(args)

    DataBases.values.foreach { dbName =>
      val strs = generateData(params.isInitLoad, dbName, params.settings).mkString("\n")
      val outpath = getOutputPath(params.isInitLoad, dbName, params.s3Path, params.dateStr)

      if (!strs.trim.isEmpty) params.s3Client.putObject(params.s3Bucket, outpath, strs)
    }
  }

  private def setUpParams(args: Array[String]): Params = {
    val isInitOption = AddOptions("i", "init", "Is this init data load. EG: False")
    val endRangeOption = AddOptions("e", "range_end", "How many records to mock")
    val dateOption = AddOptions("d", "date", "Process date; EG: 2020-06-20")
    val pathOption = AddOptions("p", "path", "S3 data output path")
    val s3BucketOption = AddOptions("s3b", "s3Bucket", "S3 bucket name")
    val s3KeyOption = AddOptions("s3k", "s3key", "S3 key")
    val s3SecOption = AddOptions("s3s", "s3sec", "S3 secret")
    val rsUrlOption = AddOptions("rsurl", "redshift-url", "Redshift URL")
    val rsUNOption = AddOptions("rsun", "redshift-username", "Redshift user name")
    val rsPWOption = AddOptions("rspw", "redshift-password", "Redshift password")

    val map = ArgsHelper.createArgs(
      args,
      Vector(
        isInitOption,
        endRangeOption,
        dateOption,
        pathOption,
        s3BucketOption,
        s3KeyOption,
        s3SecOption,
        rsUrlOption,
        rsUNOption,
        rsPWOption,
      ),
    )

    val formatter = DateTimeFormat.forPattern("yyyy-MM-dd").withZone(DateTimeZone.UTC)

    val isInit = map(isInitOption.symbol).toBoolean
    val endRange = map(endRangeOption.symbol).trim.toInt
    val dateStr = map(dateOption.symbol)
    val date = formatter.parseDateTime(dateStr).toDate
    val path = map(pathOption.symbol)
    val s3Bucket = map(s3BucketOption.symbol)
    val s3Key = map(s3KeyOption.symbol)
    val s3Sec = map(s3SecOption.symbol)
    val rsUrl = map(rsUrlOption.symbol)
    val rsUN = map(rsUNOption.symbol)
    val rsPW = map(rsPWOption.symbol)

    val s3Client = AmazonS3ClientBuilder
      .standard()
      .withRegion(Regions.US_EAST_1)
      .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(s3Key, s3Sec)))
      .build()

    val dataSettings = DataSettings(
      rangeEnd = endRange,
      seed = date,
      minMax = if (isInit) MinMaxRange() else createMinMax(rsUrl, rsUN, rsPW),
    )

    Params(dateStr, date, s3Bucket, path, s3Client, dataSettings, isInit)
  }

  private def createMinMax(rsUrl: String, rsUN: String, rsPW: String): MinMaxRange = {
    Class.forName("com.amazon.redshift.jdbc42.Driver")
    val conn = DriverManager.getConnection(rsUrl, rsUN, rsPW)
    val minMaxRange = getMinMaxFromRedshift(conn)
    conn.close()
    minMaxRange
  }

  private def getOutputPath(isInitLoad: Boolean, dbName: DataBases.Value, s3Path: String, dateStr: String): String =
    if (isInitLoad) {
      Vector(s3Path, dbName.toString.toLowerCase + ".csv").mkString("/")
    } else {
      Vector(s3Path, dateStr, dbName.toString.toLowerCase + ".csv").mkString("/")
    }

}
