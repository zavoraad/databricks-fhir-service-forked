package com.databricks.industry.solutions.fhirapi

import org.scalatest.funsuite.AnyFunSuite
import com.typesafe.config.ConfigFactory
import com.databricks.industry.solutions.fhirapi.datastore.{ServicePrincipalAuth, TokenAuth}
import com.databricks.industry.solutions.fhirapi.ZeroBusClient
import com.google.protobuf.Message
import scala.concurrent.ExecutionContext.Implicits.global

class BaseTest extends AnyFunSuite{
  def config = ConfigFactory.load()
  def ta: TokenAuth = TokenAuth(config.getString("databricks.warehouse.usertoken.auth.jdbc"), config.getString("databricks.warehouse.usertoken.auth.token"))
  def spa: ServicePrincipalAuth = ServicePrincipalAuth(
    config.getString("databricks.warehouse.serviceprincipal.auth.jdbc"),
    config.getString("databricks.warehouse.serviceprincipal.auth.http_path"),
    config.getString("databricks.warehouse.serviceprincipal.auth.client_id"),
    config.getString("databricks.warehouse.serviceprincipal.auth.client_secret"),
    config.getString("databricks.warehouse.serviceprincipal.auth.url")
    
  )
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
