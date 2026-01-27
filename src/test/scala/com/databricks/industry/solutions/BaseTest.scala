package com.databricks.industry.solutions.fhirapi

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import com.typesafe.config.ConfigFactory
import com.databricks.industry.solutions.fhirapi.datastore.{ServicePrincipalAuth, TokenAuth}
import com.databricks.industry.solutions.fhirapi.ZeroBusClient
import com.google.protobuf.Message
import scala.concurrent.ExecutionContext.Implicits.global
import ch.qos.logback.classic.{Level, LoggerContext}
import org.slf4j.LoggerFactory

class BaseTest extends AnyFunSuite with BeforeAndAfterAll{
  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val loggerContext = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
    loggerContext.getLogger("com.zaxxer.hikari").setLevel(Level.OFF)
    loggerContext.getLogger("com.databricks.zerobus").setLevel(Level.OFF)
    loggerContext.getLogger("io.grpc.netty.shaded.io").setLevel(Level.OFF)
    loggerContext.getLogger("com.github.dockerjava").setLevel(Level.OFF)
    
  }
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
