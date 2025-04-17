package com.databricks.industry.solutions.fhirapi

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import java.sql.Connection

class PoolDataStore(val auth: Auth, val conRetries: Int=1, val queryRetries: Int =1, val minIdle: Int=1, val maxPoolSize: Int = 10)  extends DataStore{

  //TODO handle token or other config...
  private val authConfig = new HikariConfig()
  authConfig.setMinimumIdle(minIdle)
  authConfig.setMaximumPoolSize(maxPoolSize)

  val hds = new HikariDataSource(authConfig)

  override def getConnection: Connection = {
    ???
  }

  protected def connect: Connection = {
    ???
  }

  override def disconnect: Unit = {
    ???
  }
}
