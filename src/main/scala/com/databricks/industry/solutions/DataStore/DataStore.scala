package com.databricks.industry.solutions.fhirapi

import java.sql.Connection

trait DataStore{
  val auth: Auth
  val conRetries: Int
  val queryRetries: Int

  //https://build.fhir.org/http.html#search
  def execute(query: String, retries: Int, con: Connection): (List[Map[String, String]], Option[String]) = {
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

  protected def connect: Connection //internal class connection handling
  def getConnection: Connection //function to interface with 
  def disconnect: Unit

  def executePaged(query: String, retries: Int): Unit = {} //todo return iterator for paging
}

