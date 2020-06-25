package com.comp.etl.spark.transform
import com.comp.etl.mock.database.model.WebLog
import com.comp.utils.WebLogUtils
import org.apache.spark.sql.functions.{col, from_unixtime}
import org.apache.spark.sql.{Column, DataFrame}

object WeblogEtl extends TransformEtl {
  override val outputCols: Vector[Column] = Vector.empty

  override def trans(input: DataFrame): DataFrame = {
    val spark = input.sparkSession

    import spark.implicits._

    input
      .mapPartitions { part =>
        part
          .map(rawWeblog => WebLog.parse(rawWeblog.toString()))
          .filter(_.isDefined)
          .map { parsedWebLog =>
            val webLog = parsedWebLog.get
            val neededInfo = WebLogUtils(webLog.clientIp, webLog.userAgent, webLog.dateTime)
            (webLog.userName, neededInfo.getTime, neededInfo.getCntry, neededInfo.getDeviceName)
          }
      }
      .toDF("user_name", "unix_time", "country", "device")
      .withColumn("created_date", from_unixtime(col("unix_time")))
      .drop(col("unix_time"))
  }
}
