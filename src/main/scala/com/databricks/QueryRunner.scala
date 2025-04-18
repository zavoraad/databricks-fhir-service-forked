package com.databricks.industry.solutions.fhirapi

import java.sql.Connection
import org.joda.time.DateTime
import java.util.{Date, UUID}
import ujson.Obj


class QueryRunner(val ds: DataStore, val queryRetries: Int = 1) {
  def runQuery(queryInput: QueryInput): QueryOutput = {
    val queryStartTime = DateTime.now
    val (results, error) = ds.execute(queryInput.query, queryRetries, ds.getConnection)
    QueryOutput(results, System.currentTimeMillis() - queryStartTime.getMillis, queryStartTime, error, queryInput.query)
  }
}

case class QueryInput(query: String)

case class QueryOutput(
  queryResults: List[Map[String, String]],
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
}


// can be moved to a new file
case class FormattedOutput(queryOutput: QueryOutput, bundle: String)

object FormattedOutput {
  def fromQueryOutputSearch(queryOutput: QueryOutput): FormattedOutput = {
    FormattedOutput(queryOutput,
      """{"resourceType": "Bundle","type":"searchset","entry":[ 
      """ +
        queryOutput.queryResults.flatMap(x => {
          x.map { case (key, value) =>
            val j = ujson.read(value)
            j("resourceType") = key
            Obj("resource" -> j, "fullUrl" -> {"urn:uuid:" + j("id").value})
          }
        }).mkString(",") +
        """]}"""
    )
     
      /*
"""
{
  "fullUrl": "urn:uuid:",
  "resource": {
    "resourceType": """ + $x[0] + """,
    x[1]
   }
}
"""
     })

     */
    /*
    queryResults.
    ujson.read(

      val bundle = new Bundle()
    bundle.setType(Bundle.BundleType.SEARCHSET)
    bundle.setId(UUID.randomUUID().toString)
    bundle.setTimestamp(new Date())

    if (queryOutput.queryResults.nonEmpty) {
      queryOutput.queryResults.foreach { row =>
        row.foreach {
          case (_, rawJson) => parseAndAddToBundle(rawJson, bundle)
        }
      }
    }

    bundle.setTotal(bundle.getEntry.size())
     FormattedOutput(queryOutput, parser.encodeResourceToString(bundle))
     */
  }
}

