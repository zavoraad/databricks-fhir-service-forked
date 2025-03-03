package com.databricks.industry.solutions.fhirapi


import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import io.github.cdimascio.dotenv.Dotenv
import java.sql.Connection
import org.slf4j.LoggerFactory
import scala.compiletime.uninitialized

object DatabaseConnectionPool {
  private val logger = LoggerFactory.getLogger(this.getClass)

  // Load environment variables from .env file
  private val dotenv = Dotenv.configure().ignoreIfMissing().load()

  private val jdbcUrl = dotenv.get("DATABRICKS_JDBC_URL")
  private val token = dotenv.get("DATABRICKS_TOKEN")

  if (jdbcUrl == null || token == null) {
    throw new RuntimeException("DATABRICKS_JDBC_URL or DATABRICKS_TOKEN is not set in .env!")
  }

  logger.info(s"Using JDBC URL: $jdbcUrl")

  private val hikariConfig = new HikariConfig()
  hikariConfig.setJdbcUrl(jdbcUrl)
  hikariConfig.setUsername("token")
  hikariConfig.setPassword(token)

  hikariConfig.setMaximumPoolSize(50)
  hikariConfig.setMinimumIdle(10)
  hikariConfig.setIdleTimeout(60000)
  hikariConfig.setMaxLifetime(600000)
  hikariConfig.setConnectionTimeout(30000)

  // Declare `dataSource` outside try-catch
  private var dataSource: HikariDataSource = uninitialized

  try {
    dataSource = new HikariDataSource(hikariConfig)
    logger.info("Database connection pool initialized successfully.")
  } catch {
    case e: Exception =>
      logger.error("Failed to initialize connection pool", e)
      throw e
  }

  // Ensure `dataSource` is accessible outside the try block
  def getConnection: Connection = dataSource.getConnection
}
