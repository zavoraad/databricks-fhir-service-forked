package com.databricks.industry.solutions.fhirapi

import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import akka.http.scaladsl.model.Uri
import akka.event.{LoggingAdapter,Logging}
import com.databricks.industry.solutions.fhirapi.queries._
import ujson._

import scala.concurrent.ExecutionContext.Implicits.global


class ServiceManager(val qi: QueryInterpreter, val qr: QueryRunner, sqlAlias: Option[BaseAlias] = None)(implicit val executor: ExecutionContext) {

  def read(typeSeg: String, idSeg: String)(implicit url: Uri): Future[FormattedOutput] = Future {
    val sql = qi.read(typeSeg, idSeg, url.query().toMap)
    val result = qr.runQuery(QueryInput(sql, url, url.toString()))
    result.error match {
      case Some(x) => FormatManager.ErrorDefault(Seq(result))
      case None => FormatManager.fromResultsNDJson(Seq(result),
        FormatManager.resourcesAsNDJSON, None, sqlAlias)
    }    
  }

  def insert(typeSeg: String, idSeg: String, payload: String)(implicit url: Uri): Future[FormattedOutput] = {
    Future.failed(new NotImplementedError("Insert not implemented"))
  }

  def getEverything(patientId: String)(implicit url: Uri): Future[FormattedOutput] = {
    // 1. Run the metadata query to find relevant tables/columns
    val metadataQuery = qi.tablesWithColumns(Seq("subject", "patient", "beneficiary", "individual"))
    
    Future {
      qr.runQuery(QueryInput(metadataQuery, url, url.toString()))
    }.flatMap { metadataResult =>
      metadataResult.error match {
        case Some(_) => Future.successful(FormatManager.ErrorDefault(Seq(metadataResult)))
        case None =>
          // 2. Generate list of data queries
          val info = metadataResult.queryResults.map(row => (row.result.get("table_name").get, row.result.get("column_name").get))
          val queries = qi.readEverythingForPatient(patientId, info)

          // 3. Run all data queries in parallel (Pass 1)
          val futurePass1Results = Future.traverse(queries) { query =>
            Future {
              qr.runQuery(QueryInput(query, url, url.toString()))
            }
          }

          // 4. Extract references for Pass 2 (e.g., Practitioners from Encounters)
          futurePass1Results.flatMap { pass1Results =>
            val practitionerQueries = pass1Results.flatMap { qo =>
              // Look specifically for results coming from an 'Encounter' query
              if (qo.queryInput.toLowerCase.contains("encounter")) {
                qo.queryResults.flatMap { row =>
                  try {
                    // Each row contains a JSON string of the resource
                    val jsonStr = row.result.values.head
                    val json = ujson.read(jsonStr)
                    
                    // FHIR Path: participant.individual.reference
                    json("participant").arr.flatMap { p =>
                      val ref = p("individual")("reference").str
                      if (ref.startsWith("Practitioner/")) {
                        // Case 1: Simple logical ID reference
                        Some(qi.read("Practitioner", ref.stripPrefix("Practitioner/"), Map.empty))
                      } else if (ref.startsWith("Practitioner?")) {
                        // Case 2: Conditional reference (search-style)
                        // Example: "Practitioner?identifier=http://hl7.org/fhir/sid/us-npi|9999995597"
                        val queryStr = ref.stripPrefix("Practitioner?")
                        val params = queryStr.split("&").flatMap { pair =>
                          val parts = pair.split("=")
                          if (parts.length >= 2) {
                            val key = parts(0)
                            val rawValue = java.net.URLDecoder.decode(parts(1), "UTF-8")
                            Some(key -> rawValue)
                          } else None
                        }.toMap
                        Some(qi.searchWithArrayFilter("Practitioner", params))
                      } else None
                    }
                  } catch {
                    case _: Exception => Nil
                  }
                }
              } else Nil
            }.distinct

            if (practitionerQueries.nonEmpty) {
              // 5. Run Pass 2 queries for referenced Practitioners
              val futurePass2Results = Future.traverse(practitionerQueries) { query =>
                Future {
                  qr.runQuery(QueryInput(query, url, url.toString()))
                }
              }
              
              futurePass2Results.map(pass2 => pass1Results ++ pass2)
            } else {
              Future.successful(pass1Results)
            }
          }.map { allResults =>
            // 6. Format the combined results from both passes
            allResults.filter(qo => qo.error != None) match {
              case l if l.size > 0 => FormatManager.ErrorDefault(allResults)
              case _ =>
                FormatManager.fromResultsBundle(
                  allResults,
                  FormatManager.resourcesAsBundle,
                  None,
                  "searchset",
                  sqlAlias
                )
            }
          }
      }
    }
  }

  def search(typeSeg: String, uri: Map[String, String]): FormattedOutput = { //TODO FormattedOutput with an iterator instead of a list 
    val sql = qi.search(typeSeg, uri) // Builds out the search query to run 
    //run query... 
    //return a paged result (cursor implementation)
    ???
  }

  def allTablesInSchema: Seq[String] = {
    qr.runQuery(QueryInput(qi.allTablesInSchema))
      .queryResults.map(row => row.result.getOrElse("tableName", ""))
      .filter(_!="")
  }

  def tableColumns(tableName: String): Seq[String] = {
    qr.runQuery(QueryInput(qi.tableSchema(tableName)))
      .queryResults.map(row => row.result.getOrElse("column_name", ""))
      .filter(_!="")
  }

  def tablesWithColumns(columns: Seq[String]): Seq[(String, String)] = {
    qr.runQuery(QueryInput(qi.tablesWithColumns(columns), Uri("")))
      .queryResults.map(row => (row.result.getOrElse("table_name", ""), row.result.getOrElse("column_name", "")))
      .filter( {case(t,_) => t != ""})
  }
}

