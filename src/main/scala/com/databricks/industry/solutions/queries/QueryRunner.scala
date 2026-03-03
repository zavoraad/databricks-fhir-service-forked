package com.databricks.industry.solutions.fhirapi.queries

import com.databricks.industry.solutions.fhirapi.datastore.DataStore
import java.sql.Connection
import org.joda.time.DateTime
import java.util.{Date, UUID}
import ujson.Obj
import akka.http.scaladsl.model.Uri


class QueryRunner(val ds: DataStore, val queryRetries: Int = 1) {
  def runQuery(queryInput: QueryInput): QueryOutput = {
    val queryStartTime = DateTime.now
    val (results, error) = ds.execute(queryInput.query, queryRetries, ds.getConnection)
    val wrappedResults = results.map(QueryResultRow(_))
    QueryOutput(wrappedResults, 
      System.currentTimeMillis() - queryStartTime.getMillis, 
      queryStartTime, 
      error, 
      queryInput.query,
      if (queryInput.fullUrl.nonEmpty) queryInput.fullUrl else queryInput.url.toString) // Capture the full URL string
  }

  // For DELETE, INSERT, UPDATE statements
  def runUpdate(queryInput: QueryInput): QueryOutput = {
    val queryStartTime = DateTime.now
    val (results, error) = ds.executeUpdate(queryInput.query, queryRetries, ds.getConnection)
    val wrappedResults = results.map(QueryResultRow(_))
    QueryOutput(wrappedResults, 
      System.currentTimeMillis() - queryStartTime.getMillis, 
      queryStartTime, 
      error, 
      queryInput.query,
      if (queryInput.fullUrl.nonEmpty) queryInput.fullUrl else queryInput.url.toString)
  }
}

case class QueryInput(query: String, url: Uri = Uri(""), fullUrl: String = "")

case class QueryResultRow(result: Map[String, String])

case class QueryOutput(
  queryResults: List[QueryResultRow], //rows of column name, value results
  queryRuntime: Long,
  queryStartTime: DateTime,
  error: Option[String],
  queryInput: String,
  url: String // Added full URL string
) {
  override def toString: String = {
    s"""queryRuntime (in ms): $queryRuntime
       |queryStartTime: $queryStartTime
       |queryError: ${error.getOrElse("None")}
       |numRows: ${queryResults.length}
       |queryExecuted: $queryInput
       |url: $url
       |data: ${queryResults.map(_.result)}
       |""".stripMargin
  }

  //Give a more parsable format to traverse and save off into a table
  def info: String = {
    //TODO
    toString
  }
}


