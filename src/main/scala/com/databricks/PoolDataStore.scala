package com.databricks.industry.solutions.fhirapi

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import java.sql.Connection

//TODO any additional settings make class variables
class PoolDataStore(val auth: Auth, val conRetries: Int=1, val queryRetries: Int =1, val minIdle: Int=1, val maxPoolSize: Int = 10)  extends DataStore{

  //TODO any additional connection variables add below for config
  private val authConfig = new HikariConfig()
  authConfig.setMinimumIdle(minIdle)
  authConfig.setMaximumPoolSize(maxPoolSize)

  val hds = new HikariDataSource(authConfig)

  //TODO give me one connection from the pool 
  override def getConnection: Connection = {
    ???
  }

  //TODO tell me how you want to authenticate given that you have an "Auth" class
  protected def connect: Connection = {
    ???
  }

  override def disconnect: Unit = {
    ???
  }
}
