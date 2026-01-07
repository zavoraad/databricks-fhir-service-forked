package com.databricks.industry.solutions.fhirapi

import org.scalatest.funsuite.AnyFunSuite
import com.typesafe.config.ConfigFactory
import com.databricks.industry.solutions.fhirapi.datastore.TokenAuth
import com.databricks.industry.solutions.fhirapi.ZeroBusClient
import com.google.protobuf.Message
import scala.concurrent.ExecutionContext.Implicits.global

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

  def zClient : ZeroBusClient =  new ZeroBusClient(
        config.getString("logging.zerobus.serverEndpoint"),
        config.getString("logging.zerobus.workspaceUrl"),
        config.getString("logging.zerobus.clientId"),
        config.getString("logging.zerobus.clientSecret"),
        config.getString("logging.zerobus.tablename"),
        Class.forName("com.databricks.industry.solutions.fhirapi.Record$" + 
      config.getString("logging.zerobus.tableProtoBuf")).asInstanceOf[Class[? <: Message]]
      )
}
