package com.comp.etl.mock.database.model

object DataBases extends Enumeration {
  type DataBase = Value

  // Order is coming from lead file
  val Companies, Customers, Orders, Products, CompanyCsv, LeadCsv = Value
}
