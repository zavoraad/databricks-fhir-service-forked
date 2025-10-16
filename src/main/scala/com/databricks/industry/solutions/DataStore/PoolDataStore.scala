package com.databricks.industry.solutions.fhirapi

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import java.sql.Connection
import scala.util.Using

class PoolDataStore(val auth: Auth, val conRetries: Int=1, val queryRetries: Int =1, val minIdle: Int=1, val maxPoolSize: Int = -1, val timeoutMS: Int = 30000)  extends DataStore{

  override def execute(query: String, retries: Int, con: Connection): (List[Map[String, String]], Option[String]) = {
    try {
      val statement = try { con.createStatement } catch { case e: Exception => throw e}
      val resultSet = try { statement.executeQuery(query) } catch { case e: Exception => 
            statement.close
            throw e
          }
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
      case e: Exception => 
        if (retries > 0) 
          execute(query, retries - 1, con) 
        else {
          (Nil, Some(e.toString)) 
        }
    } 
    finally {
      con.close
    }
  }
  private val authConfig = new HikariConfig()
  authConfig.setMinimumIdle(minIdle)
  maxPoolSize match {
    case x if x <=0 => authConfig.setMaximumPoolSize(Runtime.getRuntime().availableProcessors() * 2) //default max pool size to 2x # of CPUs
    case _ => authConfig.setMaximumPoolSize(maxPoolSize)
  }
  authConfig.setDriverClassName("com.databricks.client.jdbc.Driver")
  auth match {
    case a : TokenAuth =>
      authConfig.setUsername("token")
      authConfig.setPassword(a.token)
      authConfig.setJdbcUrl(a.jdbcURL)
  }
  authConfig.setConnectionTimeout(timeoutMS)

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
