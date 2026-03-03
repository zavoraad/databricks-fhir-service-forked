package com.databricks.industry.solutions.fhirapi

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import scala.collection.immutable.List
import ujson.Obj
import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import akka.http.scaladsl.model.Uri
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
case class FormattedOutput(
    queryOutput: Seq[QueryOutput],
    data: String,
    statusCd: StatusCode = StatusCodes.OK
) {
  def info: String = {
    s"""statusCode:""" + statusCd + "\n" +
      s"""numQueries:""" + queryOutput.length + "\n" +
      s"""queryOutputs: """ + queryOutput.map(_.toString)
  }
}

object FormatManager {

  protected def formatException(
      results: Seq[QueryOutput],
      sqlAlias: Option[BaseAlias],
      errorMessage: String,
      e: Exception
  ): FormattedOutput = {
    FormattedOutput(
      results,
      """{
            "error": """" + errorMessage + """",
            "message": """ + e.toString + """,
            "query" : """ + results
        .map(qo => qo.queryInput)
        .mkString("\n") + """,
            "data" : """ + results
        .map(qo => qo.queryResults)
        .mkString("\n") + """,
            "sqlAlias": """ + sqlAlias.toString + """
            }
            """,
      StatusCodes.InternalServerError
    )
  }

  def ErrorDefault(qol: Seq[QueryOutput]): FormattedOutput = {
    FormattedOutput(
      qol,
      """{
                "error": "Bad Request",
                "message": """ + "\"" + qol
        .filter(qo => qo.error != None)
        .map(qo => qo.error)
        .mkString("\n") + """\"",
                "query" : """ + qol
        .filter(qo => qo.error != None)
        .map(qo => qo.queryInput)
        .mkString("\n") + """, 
            }""",
      StatusCodes.BadRequest
    )
  }

  def fromResultSearch(
      results: Seq[QueryOutput],
      columns: Option[Seq[String]],
      sqlAlias: Option[BaseAlias],
      url: Uri,
      pageSize: Int
  ): FormattedOutput = {
    try {
      FormattedOutput(
        results,
        ujson.write(
          Obj(
            "resourceType" -> "Bundle",
            "total" -> "none",
            "type" -> "searchset",
            "link" -> {
              Seq(Obj("relation" -> "self", "url" -> url.toString)) ++
                {
                  // TODO find the right calculation
                  results
                    .map(qo => qo.queryResults.length)
                    .sum > pageSize match {
                    case true =>
                      Seq(
                        Obj(
                          "relation" -> "next",
                          "url" -> nextPagedUrl(url, results.last)
                        )
                      )
                    case false => Seq.empty
                  }
                }
            },
            "entry" -> results
              .take(pageSize)
              .flatMap(qo => resourceAsEntry(qo, columns, sqlAlias))
          )
        )
      )

    } catch {
      case e: Exception =>
        formatException(
          results,
          sqlAlias,
          "Unable to properly format results from query",
          e
        )
    }
  }

  def nextPagedUrl(url: Uri, qo: QueryOutput): String = {
    val params = url.query().toMap
    val updated = params - "_page" - "last_id" + ("_page" -> {
      params
        .getOrElse("_page", "1")
        .toInt + 1
    }.toString) + ("last_id" -> {
      val j = ujson.read(
        qo.queryResults.last.result
          .get(resourceTypeFromFhirUrl(url))
          .getOrElse(
            throw new Exception(
              "Unable to find an ID from a paged result for the next link"
            )
          )
      )
      j("id").str
    })
    url.withQuery(Uri.Query(updated.toSeq: _*)).toString
  }

  /** Extracts the base FHIR resource type from a FHIR URL path (e.g. Patient,
    * Encounter, Claim). The URL is expected to follow the form
    * [base]/fhir/[resourceType] or [base]/fhir/[resourceType]/...
    *
    * @param url
    *   the request URI (e.g. from the link "url" in a Bundle, or nextPagedUrl)
    * @return
    *   the resource type segment after /fhir/
    * @throws IllegalArgumentException
    *   if the path does not match the expected /fhir/[resourceType] form
    */
  def resourceTypeFromFhirUrl(url: Uri): String = {
    val segments = url.path.toString.split("/").filter(_.nonEmpty)
    if (segments.length >= 2 && segments(0).equalsIgnoreCase("fhir"))
      segments(1)
    else
      throw new IllegalArgumentException(
        s"FHIR URL path does not contain a resource type: ${url.path}"
      )
  }

  /** Extracts the base FHIR resource type from a FHIR URL string (e.g. a
    * nextPagedUrl).
    *
    * @param urlString
    *   the full URL string (e.g.
    *   "http://localhost:9000/fhir/Patient?_page=2&last_id=xyz")
    * @return
    *   the resource type (e.g. "Patient")
    * @throws IllegalArgumentException
    *   if the URL cannot be parsed or does not match /fhir/[type]
    */
  def resourceTypeFromFhirUrl(urlString: String): String =
    try {
      resourceTypeFromFhirUrl(Uri(urlString))
    } catch {
      case e: IllegalArgumentException => throw e
      case e: Exception                =>
        throw new IllegalArgumentException(
          s"Invalid FHIR URL or path does not contain a resource type: $urlString",
          e
        )
    }

  def fromResultsDelete(
      results: Seq[QueryOutput],
      f: (Seq[QueryOutput], Option[Seq[String]], Option[BaseAlias]) => String,
      columns: Option[Seq[String]],
      sqlAlias: Option[BaseAlias]
  ): FormattedOutput = {
    try {
      {
        results
          .flatMap(_.queryResults)
          .flatMap { row =>
            row.result.get("num_affected_rows").flatMap(_.toIntOption)
          }
          .sum
      } match {
        case n if n > 0 =>
          // Resource was found and deleted - return 200 OK with OperationOutcome
          FormattedOutput(
            results,
            ujson.write(
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
            ),
            StatusCodes.OK
          )

        case _ =>
          // Resource not found - return 204 No Content with empty body
          // Per FHIR spec: DELETE on a non-existent resource returns 204 No Content
          FormattedOutput(results, "", StatusCodes.NoContent)
      }
    } catch {
      case e: Exception =>
        formatException(
          results,
          sqlAlias,
          "Unable to properly format delete results",
          e
        )
    }
  }

  def fromResultsNDJson(
      results: Seq[QueryOutput],
      f: (Seq[QueryOutput], Option[Seq[String]], Option[BaseAlias]) => String,
      columns: Option[Seq[String]],
      sqlAlias: Option[BaseAlias]
  ): FormattedOutput = {
    try { FormattedOutput(results, f(results, columns, sqlAlias)) }
    catch {
      case e: Exception =>
        formatException(
          results,
          sqlAlias,
          "Unable to properly format results from query",
          e
        )
    }
  }

  def fromResultsBundle(
      results: Seq[QueryOutput],
      f: (
          Seq[QueryOutput],
          Option[Seq[String]],
          String,
          Option[BaseAlias]
      ) => String,
      columns: Option[Seq[String]],
      transactionType: String = "searchset",
      sqlAlias: Option[BaseAlias]
  ): FormattedOutput = {
    try {
      FormattedOutput(results, f(results, columns, transactionType, sqlAlias))
    } catch {
      case e: Exception =>
        formatException(
          results,
          sqlAlias,
          "Unable to properly format results from query",
          e
        )
    }
  }

  def time(): String = {
    ZonedDateTime
      .now()
      .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
  }

  def resourcesAsNDJSON(
      qol: Seq[QueryOutput],
      columns: Option[Seq[String]] = None,
      sqlAlias: Option[BaseAlias] = None
  ): String = {
    qol.map(qo => resourceAsNDJSON(qo, columns, sqlAlias)).mkString("\n")
  }

  /*
        turn queryResults into ndjson
   */
  def resourceAsNDJSON(
      qo: QueryOutput,
      columns: Option[Seq[String]] = None,
      sqlAlias: Option[BaseAlias] = None
  ): String = {
    columns match {
      case Some(c) => ???
      case None    => {
        qo.queryResults
          .flatMap(row =>
            row.result.values
              .filter(s => s.length > 0)
              .map(x => ujson.read(x))
              .map(j => ujsonWithAlias(j, sqlAlias))
          )
          .mkString("\n")
      }
    }
  }

  def resourcesAsBundle(
      qol: Seq[QueryOutput],
      columns: Option[Seq[String]] = None,
      transactionType: String = "searchset",
      sqlAlias: Option[BaseAlias] = None
  ): String = {
    ujson.write(
      Obj(
        "resourceType" -> "Bundle",
        "type" -> transactionType,
        "entry" -> qol.flatMap(qo => resourceAsEntry(qo, columns, sqlAlias))
      )
    )
  }

  /*
        This method should only be called by this class as it does not
        construct the wrapped data needed for a bundle
   */
  def resourceAsEntry(
      qo: QueryOutput,
      columns: Option[Seq[String]] = None,
      sqlAlias: Option[BaseAlias] = None
  ): Seq[Obj] = {
    columns match {
      case Some(c) => ???
      case None    => { // builds entry
        qo.queryResults.flatMap(row =>
          row.result.values
            .filter(s => s.length > 0)
            .map(s => ujson.read(s))
            .map(j => ujsonWithAlias(j, sqlAlias))
            .map(j =>
              Obj("resource" -> j, "fullUrl" -> { "urn:uuid:" + j("id").value })
            )
        )
      }
    }
  }
  /*
        Updates j as a side effect
   */
  def ujsonWithAlias(
      j: ujson.Value,
      sqlAlias: Option[BaseAlias] = None
  ): ujson.Obj = {
    sqlAlias match {
      case None    => j
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
              }
            )
          }
          case _ => {}
        }
      }
    }
    j.obj
  }

}
