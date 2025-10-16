package com.databricks.industry.solutions.fhirapi

import org.scalatest.funsuite.AnyFunSuite
import com.typesafe.config.ConfigFactory
import com.databricks.industry.solutions.fhirapi.TokenAuth

class BaseTest extends AnyFunSuite{
  def config = ConfigFactory.load()
  def ta: TokenAuth = TokenAuth(config.getString("databricks.warehouse.jdbc"), config.getString("databricks.warehouse.token"))
  def canConnect: Boolean = {
    try{
      val c = ta.connect
      c.close
      return true
    }
    catch{
      case e: Exception => false
    }
  }
}
