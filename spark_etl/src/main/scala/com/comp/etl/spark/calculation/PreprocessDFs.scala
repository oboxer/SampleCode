package com.comp.etl.spark.calculation

import org.apache.spark.sql.DataFrame

case class PreprocessDFs(
    companyDF: DataFrame,
    customerDF: DataFrame,
    updatedOrdersDF: DataFrame,
    productDF: DataFrame,
    parsedWebLogDF: DataFrame,
) {
  def getPaths(dateStr: String) = Vector(
    (s"table=companies/$dateStr/", companyDF),
    (s"table=customers/$dateStr/", customerDF),
    (s"table=orders/$dateStr/", updatedOrdersDF),
    (s"table=products/$dateStr/", productDF),
    (s"table=weblogs/$dateStr/", parsedWebLogDF),
  )
}
