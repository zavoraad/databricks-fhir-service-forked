package com.databricks.industry.solutions.fhirapi

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import java.sql.Connection

class PoolDataStore(val auth: Auth, val conRetries: Int=1, val queryRetries: Int =1, val minIdle: Int=1, val maxPoolSize: Int = -1)  extends DataStore{


  override def execute(query: String, retries: Int, con: Connection): (List[Map[String, String]], Option[String]) = {
    val statement = con.createStatement
    val resultSet = statement.executeQuery(query)
    try {
      val it = new Iterator[Map[String, String]] {
        def hasNext: Boolean = resultSet.next() // Check if there are more rows
        def next(): Map[String, String] = {
          (1 to resultSet.getMetaData.getColumnCount).map { i =>
            resultSet.getMetaData.getColumnName(i) -> resultSet.getString(i)
          }.toMap
        }
      }
      (it.toList, None)
    } catch {
      case r if retries > 0 => execute(query, retries - 1, con)
      case e: Exception =>
        (Nil, Some(e.toString))
    } finally {
      if (resultSet != null) resultSet.close
      if (statement != null) statement.close
      if (con != null) con.close
    }
  }
  private val authConfig = new HikariConfig()
  authConfig.setMinimumIdle(minIdle)
  maxPoolSize match {
    case -1 => authConfig.setMaximumPoolSize({ if (Runtime.getRuntime().availableProcessors() -1 <= 0) 1 else  Runtime.getRuntime().availableProcessors() -1} ) //default max pool size to # of CPUs -1
    case 0 => authConfig.setMaximumPoolSize(1)
    case _ => authConfig.setMaximumPoolSize(maxPoolSize)
  }
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
