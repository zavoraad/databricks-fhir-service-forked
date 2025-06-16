package com.databricks.industry.solutions.fhirapi

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import java.sql.Connection

//TODO any additional settings make class variables
class PoolDataStore(val auth: Auth, val conRetries: Int=1, val queryRetries: Int =1, val minIdle: Int=1, val maxPoolSize: Int = 10)  extends DataStore{

  //TODO any additional connection variables add below for config
  private val authConfig = new HikariConfig()
  authConfig.setMinimumIdle(minIdle)
  authConfig.setMaximumPoolSize(maxPoolSize)
  authConfig.setDriverClassName("com.databricks.client.jdbc.Driver")
  auth match {
    case a : TokenAuth =>
      authConfig.setUsername("token")
      authConfig.setPassword(a.token)
      authConfig.setJdbcUrl(a.jdbcURL)
  }

  lazy val hds = new HikariDataSource(authConfig)

  override def getConnection: Connection = {
    hds.getConnection
  }

  protected def connect: Connection = {
    hds.getConnection //HDS is already handling this all for us
  }

  override def disconnect: Unit = {
    hds.close
  }
}
