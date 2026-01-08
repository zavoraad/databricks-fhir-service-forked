package com.databricks.industry.solutions.fhirapi

import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import akka.http.scaladsl.model.Uri
import akka.event.{LoggingAdapter,Logging}
import com.databricks.industry.solutions.fhirapi.queries._

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

          // 3. Run all data queries in parallel
          val futureResults = Future.traverse(queries) { query =>
            Future {
              qr.runQuery(QueryInput(query, url, url.toString()))
            }
          }

          // 4. Format the final results
          futureResults.map { results =>
            results.filter(qo => qo.error != None) match {
              case l if l.size > 0 => FormatManager.ErrorDefault(results)
              case _ =>
                FormatManager.fromResultsBundle(
                  results,
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

