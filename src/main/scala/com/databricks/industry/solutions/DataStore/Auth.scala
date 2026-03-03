package com.databricks.industry.solutions.fhirapi.datastore

import java.sql.{Connection, DriverManager}
import java.util.Properties
import java.io.{PrintWriter, StringWriter}
import org.joda.time.DateTime
import sttp.client4.*
import java.net.URI
import io.circe.generic.auto._
import sttp.client4.circe._
import sttp.client4.httpurlconnection.HttpURLConnectionBackend

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
class ServicePrincipalAuth(val jdbcUrl: String, val httpPath: String, val clientId: String, val clientSecret: String, val authUrl: String) extends Auth {
  class SLToken(val token: String, val expiryTime: DateTime){
    //returns true if the token is expired within 60 seconds
    def expiresWithin(seconds: Int = 60): Boolean = {
      expiryTime.isBefore(DateTime.now().plusSeconds(seconds))
    }
  }
  case class SLTokenResponse(val access_token: String, val scope: String, val token_type: String, val expires_in: Int)


  private var _t: Option[SLToken] = None
  def t: SLToken = {
    _t match {
      case Some(st: SLToken) if !st.expiresWithin(60) => st
      case _ =>
        val newTok = refreshToken
        _t = Some(newTok)
        newTok
    }
  }

  def refreshToken: SLToken = {
    val request = basicRequest
      .auth.basic(clientId, clientSecret)
      .body(Map("grant_type" -> "client_credentials", "scope" -> "all-apis"))
      .post(uri"$authUrl")
      .response(asJson[SLTokenResponse]) 

      val response = request.send(HttpURLConnectionBackend())
      
      response.body match { 
        case Right(tokenResp) => new SLToken(
            tokenResp.access_token,
            DateTime.now().plusSeconds(tokenResp.expires_in)
          )
        case Left(error) => throw new RuntimeException(s"Token request failed: $error")
      }
  }

  def connect: Connection = {
    Class.forName("com.databricks.client.jdbc.Driver")
    val props = new Properties()
    props.setProperty("httpPath", httpPath)
    props.setProperty("AuthMech", "11")
    props.setProperty("Auth_Flow", "0")
    props.setProperty("Auth_AccessToken", t.token)
    try {
      DriverManager.getConnection(jdbcUrl, props)
    } catch {
      case e: Exception =>
        val sw = new StringWriter()
        e.printStackTrace(new PrintWriter(sw))
        throw new RuntimeException(
          s"ServicePrincipalAuth failed to connect. " +
            s"Check url/httpPath/token values and that the Databricks JDBC driver is on the classpath. " +
            s"Root cause: ${e.toString}\n${sw.toString}",
          e
        )
    }
  }
  
}
