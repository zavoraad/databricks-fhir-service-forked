/*
 1. optimized for multiple connections
    -JDBC Pool (monolithic tenancy)
 2. Specific input/output for queries


 6. On Behlf Of... Multi tenancy... 
 */

package com.databricks.industry.solutions.fhirapi

import java.sql.{Connection, DriverManager, ResultSet}
import org.joda.time.DateTime

// QueryInput case class
case class QueryInput(query: String)

// QueryOutput case class
//Runtime in milliseconds
case class QueryOutput(queryResults: List[Map[String, String]], queryRuntime: Long, queryStartTime: DateTime, error: Option[String]){
  override def toString : String = {
    "queryRuntime (in ms): " + queryRuntime +
    "\nqueryStartTime: " + queryStartTime +
    "\nqueryError: " + error.toString +
    "\nnumRows: " + queryResults.length
  }
}

trait Auth{
  def connect: Connection
  def disconnect(c: Connection): Unit = c.close
  def canConnect(c: Connection): Boolean = c.isValid(5)
}

class TokenAuth(val jdbcURL: String, private val token: String) extends Auth {
  def connect: Connection = {
    Class.forName("com.databricks.client.jdbc.Driver")
    DriverManager.getConnection(jdbcURL + ";UID=token;PWD=" + token)
  }
}

class QueryRunner (auth: Auth){

  //TODO Change this to connection pooling
  lazy val con = auth.connect

  // method to run the query
  def runQuery(queryInput: QueryInput): QueryOutput = {
    val queryStartTime = DateTime.now

    return {
      try {
        // Execute the query
        val statement = con.createStatement
        val resultSet = statement.executeQuery(queryInput.query)

        // Create an Iterator from the ResultSet
        val iterator = new Iterator[Map[String, String]] {
          def hasNext: Boolean = resultSet.next() // Check if there are more rows
          def next(): Map[String, String] = {
            (1 to resultSet.getMetaData.getColumnCount).map{ i =>
              resultSet.getMetaData.getColumnName(i) -> resultSet.getString(i)
            }.toMap
          }
        }

        // Collect results into a list using the iterator and map function
        // Calculate runtime in milliseconds
        QueryOutput(iterator.toList, System.currentTimeMillis() - queryStartTime.getMillis, queryStartTime, None)

      } catch {
        case e: Exception =>
          QueryOutput(List[Map[String, String]](), System.currentTimeMillis() - queryStartTime.getMillis, queryStartTime, Some(e.toString))
      }
    }
  }
}
