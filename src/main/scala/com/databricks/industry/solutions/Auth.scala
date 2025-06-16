package com.databricks.industry.solutions.fhirapi

import java.sql.{Connection,DriverManager}

trait Auth{
  def connect: Connection
  def disconnect(c: Connection): Unit = c.close
  def canConnect(c: Connection): Boolean = c.isValid(5)
}

class TokenAuth(val jdbcURL: String, val token: String) extends Auth {
  def connect: Connection = {
    Class.forName("com.databricks.client.jdbc.Driver")
    jdbcURL.takeRight(1) match {
      case ";" =>     DriverManager.getConnection(jdbcURL + "UID=token;PWD=" + token)
      case _ =>  DriverManager.getConnection(jdbcURL + ";UID=token;PWD=" + token)
    }
  }
}
