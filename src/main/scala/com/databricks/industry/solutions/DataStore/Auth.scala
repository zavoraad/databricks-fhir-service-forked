package com.databricks.industry.solutions.fhirapi.datastore

import java.sql.{Connection, DriverManager}
import java.util.Properties

/**
  * Defines a generic authentication trait for establishing database connections.
  */
trait Auth{
  /**
    * Establishes a connection to the database.
    *
    * @return A `java.sql.Connection` object.
    */
  def connect: Connection
  /**
    * Closes an existing database connection.
    *
    * @param c The `java.sql.Connection` to close.
    */
  def disconnect(c: Connection): Unit = c.close
  /**
    * Checks if a connection is valid.
    *
    * @param c The `java.sql.Connection` to validate.
    * @return `true` if the connection is valid, `false` otherwise.
    */
  def canConnect(c: Connection): Boolean = c.isValid(5)
}

/**
  * Implements token-based authentication for a Databricks JDBC connection.
  *
  * @param jdbcURL The JDBC URL for the Databricks cluster.
  * @param token   The personal access token for authentication.
  */
class TokenAuth(val jdbcURL: String, val token: String) extends Auth {
  /**
    * Creates a JDBC connection to a Databricks cluster using a personal access token.
    * It ensures the JDBC driver is loaded and appends authentication details to the JDBC URL.
    *
    * @return A `java.sql.Connection` object to the Databricks cluster.
    */
  def connect: Connection = {
    Class.forName("com.databricks.client.jdbc.Driver")
    val props = new Properties()
    props.setProperty("UID", "token")
    props.setProperty("PWD", token)
    DriverManager.getConnection(jdbcURL, props)
  }
}

//https://docs.databricks.com/aws/en/integrations/jdbc/authentication#oauth-20-tokens
class ServicePrincipalAuth(val url: String, val httpPath: String, val token: String) extends Auth {
  def connect: Connection = {
    Class.forName("com.databricks.client.jdbc.Driver")
    val props = new Properties()
    props.setProperty("httpPath", httpPath)
    props.setProperty("AuthMetch", "11")
    props.setProperty("Auth_Flow", "0")
    props.setProperty("Auth_AccessToken", token)
    DriverManager.getConnection(url, props)
  }
}
