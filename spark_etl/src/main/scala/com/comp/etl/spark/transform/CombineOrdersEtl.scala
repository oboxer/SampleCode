package com.comp.etl.spark.transform

import org.apache.spark.sql.{DataFrame, SparkSession}

object CombineOrdersEtl {

  def trans(spark: SparkSession, dataPath: String, ordersSnapshotDF: DataFrame): DataFrame= {
    val compsDF = spark.read
      .option("mode", "DROPMALFORMED")
      .schema(CompanyCsvEtl.INPUT_SCHEMA)
      .csv(s"$dataPath/companycsv.csv")
    val leadsDF = spark.read
      .option("mode", "DROPMALFORMED")
      .schema(LeadsCsvEtl.INPUT_SCHEMA)
      .csv(s"$dataPath/leadcsv.csv")

    val newOrdersDF = CompanyCsvEtl.trans(compsDF).unionByName(LeadsCsvEtl.trans(leadsDF))
    newOrdersDF.unionByName(ordersSnapshotDF)
  }
}
