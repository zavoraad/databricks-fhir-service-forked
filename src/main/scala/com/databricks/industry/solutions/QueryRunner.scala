package com.databricks.industry.solutions.fhirapi

import java.sql.Connection
import org.joda.time.DateTime
import java.util.{Date, UUID}
import ujson.Obj
import akka.http.scaladsl.model.Uri


class QueryRunner(val ds: DataStore, val queryRetries: Int = 1) {
  def runQuery(queryInput: QueryInput): QueryOutput = {
    val queryStartTime = DateTime.now
    val (results, error) = ds.execute(queryInput.query, queryRetries, ds.getConnection)
    QueryOutput(results, System.currentTimeMillis() - queryStartTime.getMillis, queryStartTime, error, queryInput.query)
  }
}

case class QueryInput(query: String, url: Uri)

case class QueryOutput(
  queryResults: List[Map[String, String]], //rows of column name, value results
  queryRuntime: Long,
  queryStartTime: DateTime,
  error: Option[String],
  queryInput: String
) {
  override def toString: String = {
    s"""queryRuntime (in ms): $queryRuntime
       |queryStartTime: $queryStartTime
       |queryError: ${error.getOrElse("None")}
       |numRows: ${queryResults.length}
       |queryExecuted: $queryInput
       |data: $queryResults
       |""".stripMargin
  }

  //Give a more parsable format to traverse and save off into a table
  def info: String = {
    //TODO
    toString
  }
}


