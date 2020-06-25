package com.comp.etl.spark.out

import org.apache.spark.sql.{DataFrame, SaveMode}

object ParquetOut {
  def fileOut(df: DataFrame, outPath: String): Unit = df.coalesce(1).write.mode(SaveMode.Overwrite).parquet("s3a://com.comp.prod.data.etl/data/final/"+outPath)
}
