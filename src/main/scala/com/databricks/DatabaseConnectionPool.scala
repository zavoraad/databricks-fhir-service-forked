package com.databricks.industry.solutions.fhirapi

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import io.github.cdimascio.dotenv.Dotenv
import java.sql.Connection
import scala.collection.concurrent.TrieMap
import org.slf4j.LoggerFactory

object DatabaseConnectionPool {

  private val logger = LoggerFactory.getLogger(this.getClass)

  // Load from .env
  private val dotenv = Dotenv.configure().ignoreIfMissing().load()
  private val jdbcUrl = dotenv.get("DATABRICKS_JDBC_URL")

  if (jdbcUrl == null || jdbcUrl.isBlank) {
    throw new RuntimeException("DATABRICKS_JDBC_URL is not set in .env file!")
  }

  // Thread-safe cache to store pools per user
  private val userPools = TrieMap[String, HikariDataSource]()

  def getConnection(userId: String, userToken: String): Connection = {
    val dataSource = userPools.getOrElseUpdate(userId, createPoolForUser(userToken))
    dataSource.getConnection
  }

  private def createPoolForUser(userToken: String): HikariDataSource = {
    val config = new HikariConfig()
    config.setJdbcUrl(jdbcUrl)
    config.setUsername("token")
    config.setPassword(userToken)
    config.setMaximumPoolSize(10)
    config.setMinimumIdle(1)

    logger.info(s"Creating JDBC pool for user.")
    new HikariDataSource(config)
  }

  def shutdownAll(): Unit = {
    userPools.values.foreach(_.close())
    userPools.clear()
    logger.info("Shut down all user JDBC pools.")
  }
}
