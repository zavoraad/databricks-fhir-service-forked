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
case class QueryInput(query: String, oboUser: String)//can delete the oboUser

// QueryOutput case class
case class QueryOutput(queryResults: List[Map[String, String]], queryRuntime: Long, queryStartTime: DateTime)//include the query that was run and querystatus, query message, query row count

class authorization()//need to be implemented

class QueryRunner {

  // JDBC connection parameters
  private val auth = "UID=token;PWD="
  private val url = "jdbc:databricks://e2-demo-field-eng.cloud.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/6e6c79dc23490ad6;" + auth

  // method to run the query
  def runQuery(queryInput: QueryInput): QueryOutput = {
    var connection: Connection = null
    var resultSet: ResultSet = null
    val queryStartTime = DateTime.now()
    var resultsList = List[Map[String, String]]() // List to hold results as maps

    try {

      // Establishing the connection - this will be later moved to it's own class and will leverage connection pooling properties
      connection = DriverManager.getConnection(url)

      // Log or use the oboUser information as needed for multi-tenancy
      println(s"Running query on behalf of user: ${queryInput.oboUser}")

      // Execute the query
      val statement = connection.createStatement()
      resultSet = statement.executeQuery(queryInput.query)

      // Create an Iterator from the ResultSet
      val iterator = new Iterator[Map[String, String]] {
        def hasNext: Boolean = resultSet.next() // Check if there are more rows
        def next(): Map[String, String] = {
          val metaData = resultSet.getMetaData
          val columnCount = metaData.getColumnCount
          (1 to columnCount).map { i =>
            metaData.getColumnName(i) -> resultSet.getString(i)
          }.toMap
        }
      }

      // Collect results into a list using the iterator and map function
      resultsList = iterator.toList

    } catch {
      case e: Exception =>
        e.printStackTrace() // Handle exceptions appropriately in production code
    } finally {
      // Clean up resources
      if (resultSet != null) resultSet.close()
      if (connection != null) connection.close()
    }

    // Calculate runtime in milliseconds
    val queryRuntime = System.currentTimeMillis() - queryStartTime.getMillis

    QueryOutput(resultsList, queryRuntime, queryStartTime)
  }
}
