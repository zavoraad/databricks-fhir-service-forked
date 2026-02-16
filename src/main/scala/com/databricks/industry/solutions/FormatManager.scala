package com.databricks.industry.solutions.fhirapi

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import scala.collection.immutable.List
import ujson.Obj
import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import com.databricks.industry.solutions.fhirapi.queries._


/* Spark DDL of this case class
CREATE TABLE ... (
  queryOutput ARRAY<STRUCT<
    queryResults: ARRAY<MAP<STRING, STRING>>,
    queryRuntime: BIGINT,
    queryStartTime: STRING,
    error: STRING,
    queryInput: STRING
  >>,
  data STRING,
  statusCd STRING
);
 */
case class FormattedOutput(queryOutput: Seq[QueryOutput], data: String, statusCd: StatusCode = StatusCodes.OK){
    def info: String = {
       s"""statusCode:""" + statusCd + "\n" +
       s"""numQueries:""" + queryOutput.length + "\n" +
       s"""queryOutputs: """ + queryOutput.map(_.toString)
    }
}

object FormatManager {

    protected def formatException(results: Seq[QueryOutput], sqlAlias: Option[BaseAlias], errorMessage: String, e: Exception): FormattedOutput = {
        FormattedOutput(results,
            """{
            "error": """" + errorMessage + """",
            "message": """ + e.toString + """,
            "query" : """ + results.map(qo => qo.queryInput).mkString("\n") + """,
            "data" : """ + results.map(qo => qo.queryResults).mkString("\n")  + """,
            "sqlAlias": """ + sqlAlias.toString + """
            }
            """,
            StatusCodes.InternalServerError
        )
    }

    def ErrorDefault(qol: Seq[QueryOutput]): FormattedOutput = {
        FormattedOutput(qol, """{
                "error": "Bad Request",
                "message": """ + "\"" + qol.filter(qo => qo.error != None).map(qo => qo.error).mkString("\n")  + """\"",
                "query" : """ + qol.filter(qo => qo.error != None).map(qo => qo.queryInput).mkString("\n") + """, 
            }""",
            StatusCodes.BadRequest
        )
    }

    def fromResultsDelete(results: Seq[QueryOutput], f: (Seq[QueryOutput], Option[Seq[String]], Option[BaseAlias]) => String, columns: Option[Seq[String]], sqlAlias: Option[BaseAlias]): FormattedOutput = {
        try {
            {results.flatMap(_.queryResults).flatMap { row =>
                row.result.get("num_affected_rows").flatMap(_.toIntOption)
            }.sum} match {
                case n if n > 0 =>
                    // Resource was found and deleted - return 200 OK with OperationOutcome
                    FormattedOutput(results, ujson.write(
                        Obj(
                            "resourceType" -> "OperationOutcome",
                            "issue" -> ujson.Arr(
                                Obj(
                                    "severity" -> "information",
                                    "code" -> "informational",
                                    "diagnostics" -> "Resource deleted"
                                )
                            )
                        )
                    )
                    , StatusCodes.OK)
                
                case _ =>
                    // Resource not found - return 204 No Content with empty body
                    // Per FHIR spec: DELETE on a non-existent resource returns 204 No Content
                    FormattedOutput(results, "", StatusCodes.NoContent)
            }
        } catch {
            case e: Exception => formatException(results, sqlAlias, "Unable to properly format delete results", e)
        }
    }

    def fromResultsNDJson(results: Seq[QueryOutput], f: (Seq[QueryOutput], Option[Seq[String]], Option[BaseAlias]) => String, columns: Option[Seq[String]], sqlAlias: Option[BaseAlias]): FormattedOutput = {
        try { FormattedOutput(results, f(results, columns,sqlAlias)) }
        catch {
            case e: Exception => formatException(results, sqlAlias, "Unable to properly format results from query", e)
        }   
    }
    
    def fromResultsBundle(results: Seq[QueryOutput], f: (Seq[QueryOutput], Option[Seq[String]], String, Option[BaseAlias]) => String, columns: Option[Seq[String]], transactionType: String = "searchset", sqlAlias: Option[BaseAlias]): FormattedOutput = {
        try { FormattedOutput(results, f(results, columns, transactionType, sqlAlias)) }
        catch {
            case e: Exception => formatException(results, sqlAlias, "Unable to properly format results from query", e)
        }   
    }

    def time(): String = {
        ZonedDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
    }

    def resourcesAsNDJSON(qol: Seq[QueryOutput], columns: Option[Seq[String]] = None, sqlAlias: Option[BaseAlias] = None): String = {
        qol.map(qo => resourceAsNDJSON(qo, columns, sqlAlias)).mkString("\n")
    }

    /*
        turn queryResults into ndjson
     */
    def resourceAsNDJSON(qo: QueryOutput, columns: Option[Seq[String]] = None, sqlAlias: Option[BaseAlias] = None): String = {
        columns match {
            case Some(c)  => ???
            case None => {
                qo.queryResults.flatMap(row => 
                    row.result.values
                    .filter(s => s.length > 0)
                    .map(x => ujson.read(x))
                    .map(j => ujsonWithAlias(j, sqlAlias))
                ).mkString("\n")
            }
        }
    }

    def resourcesAsBundle(qol: Seq[QueryOutput], columns: Option[Seq[String]] = None, transactionType: String = "searchset", sqlAlias: Option[BaseAlias] = None): String = {
        ujson.write(
            Obj("resourceType" -> "Bundle",
                "type" -> transactionType,
                "entry" -> qol.flatMap(qo => resourcesAsEntry(qo, columns, sqlAlias)))
        )
    }
        
    /* 
        This method should only be called by this class as it does not 
        construct the wrapped data needed for a bundle 
     */
    
    def resourcesAsEntry(qo: QueryOutput, columns: Option[Seq[String]] = None, sqlAlias: Option[BaseAlias] = None): Seq[Obj] = {
        columns match {
            case Some(c) => ???
            case None => { //builds entry
                qo.queryResults.flatMap(row => 
                    row.result.values
                        .filter(s => s.length > 0)
                        .map(s => ujson.read(s))
                        .map(j => ujsonWithAlias(j, sqlAlias))
                        .map(j => Obj("resource" -> j, "fullUrl" -> {"urn:uuid:" + j("id").value}))
                )
            }
        }
    }
    /* 
        Updates j as a side effect
     */
    def ujsonWithAlias(j: ujson.Value, sqlAlias: Option[BaseAlias] = None): ujson.Obj = {
        sqlAlias match {
            case None => j
            case Some(x) => {
                x.a match {
                    case Some(m) => {
                        m.map(y => 
                        j.obj.contains(y(1)) match {
                            case true => {
                                j(y(0)) = j(y(1))
                                j.obj.remove(y(1))
                            }
                            case _ => {}
                        })
                    }
                    case _ => {}
                }
            }
        }
        j.obj
    }

}
