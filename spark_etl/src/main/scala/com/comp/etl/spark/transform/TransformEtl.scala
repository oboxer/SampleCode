package com.comp.etl.spark.transform

import org.apache.spark.sql.{Column, DataFrame}

trait TransformEtl {
  val outputCols: Vector[Column]
  def trans(input: DataFrame): DataFrame
}
