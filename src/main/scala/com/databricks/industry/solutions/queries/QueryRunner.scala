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
    val con = ds.getConnection
    var ps: java.sql.PreparedStatement = null
    val queryStr = queryInput.query.toString
    try {
      ps = queryInput.query.asPreparedStatement(con)
      val rs = ps.executeQuery()
      val meta = rs.getMetaData
      val colCount = meta.getColumnCount
      val results = collection.mutable.ListBuffer.empty[Map[String, String]]
      while (rs.next()) {
        results += (1 to colCount).map { i =>
          meta.getColumnName(i) -> rs.getString(i)
        }.toMap
      }
      QueryOutput(
        results.map(QueryResultRow(_)).toList,
        System.currentTimeMillis() - queryStartTime.getMillis,
        queryStartTime,
        None,
        queryStr,
        if (queryInput.fullUrl.nonEmpty) queryInput.fullUrl
        else queryInput.url.toString
      )
    } catch {
      case e: Exception =>
        QueryOutput(
          Nil,
          System.currentTimeMillis() - queryStartTime.getMillis,
          queryStartTime,
          Some(e.toString),
          queryStr,
          if (queryInput.fullUrl.nonEmpty) queryInput.fullUrl
          else queryInput.url.toString
        )
    } finally {
      if (ps != null) ps.close()
      if (con != null) con.close()
    }
  }

  // For DELETE, INSERT, UPDATE statements
  def runUpdate(queryInput: QueryInput): QueryOutput = {
    val queryStartTime = DateTime.now
    val con = ds.getConnection
    var ps: java.sql.PreparedStatement = null
    val queryStr = queryInput.query.toString
    try {
      ps = queryInput.query.asPreparedStatement(con)
      val affectedRows = ps.executeUpdate()
      QueryOutput(
        List(QueryResultRow(Map("num_affected_rows" -> affectedRows.toString))),
        System.currentTimeMillis() - queryStartTime.getMillis,
        queryStartTime,
        None,
        queryStr,
        if (queryInput.fullUrl.nonEmpty) queryInput.fullUrl
        else queryInput.url.toString
      )
    } catch {
      case e: Exception =>
        QueryOutput(
          Nil,
          System.currentTimeMillis() - queryStartTime.getMillis,
          queryStartTime,
          Some(e.toString),
          queryStr,
          if (queryInput.fullUrl.nonEmpty) queryInput.fullUrl
          else queryInput.url.toString
        )
    } finally {
      if (ps != null) ps.close()
      if (con != null) con.close()
    }
  }
}

case class QueryInput(
    query: ParameterizedQuery,
    url: Uri = Uri(""),
    fullUrl: String = ""
)

case class QueryResultRow(result: Map[String, String])

case class QueryOutput(
    queryResults: List[QueryResultRow], // rows of column name, value results
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

  // Give a more parsable format to traverse and save off into a table
  def info: String = {
    // TODO
    toString
  }
}
