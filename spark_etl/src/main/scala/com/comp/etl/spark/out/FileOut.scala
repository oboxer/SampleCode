package com.comp.etl.spark.out

import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SaveMode}

object FileOut {
  def parquet(df: DataFrame, outPath: String): Unit =
    write(df).parquet("s3a://com.comp.prod.data.etl/data/final/" + outPath)
  def csv(df: DataFrame, outPath: String): Unit =
    write(df).option("header", "true").csv("s3a://com.comp.prod.data.etl/data/final/" + outPath)

  private def write(df: DataFrame): DataFrameWriter[Row] =
    df.coalesce(1).write.mode(SaveMode.Overwrite)
}
