/*
 1. optimized for multiple connections
    -JDBC Pool (monolithic tenancy)
 2. Specific input/output for queries


 6. On Behlf Of... Multi tenancy... 
 */
package com.databricks.industry.solutions.fhirapi


import java.sql.{Connection, ResultSet}
import org.joda.time.DateTime
import com.databricks.industry.solutions.fhirapi.DatabaseConnectionPool

// Query Input and Output case classes
case class QueryInput(query: String, oboUser: String)
case class QueryOutput(queryResults: List[Map[String, String]], queryRuntime: Long, queryStartTime: DateTime)

class QueryRunner {

  def runQuery(queryInput: QueryInput): QueryOutput = {
    val queryStartTime = DateTime.now()
    var resultsList = List[Map[String, String]]()

    // Use Pooled Connection
    val connection = DatabaseConnectionPool.getConnection
    var resultSet: ResultSet = null

    try {
      val statement = connection.createStatement()
      resultSet = statement.executeQuery(queryInput.query)

      // Convert ResultSet to List[Map[String, String]]
      val metaData = resultSet.getMetaData
      val columnCount = metaData.getColumnCount

      while (resultSet.next()) {
        val row = (1 to columnCount).map { i =>
          metaData.getColumnName(i) -> resultSet.getString(i)
        }.toMap
        resultsList = resultsList :+ row
      }

    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally {
      // Close ResultSet and return connection to pool
      if (resultSet != null) resultSet.close()
      if (connection != null) connection.close()
    }

    val queryRuntime = System.currentTimeMillis() - queryStartTime.getMillis
    QueryOutput(resultsList, queryRuntime, queryStartTime)
  }
}


