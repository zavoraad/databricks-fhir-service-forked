package com.databricks.industry.solutions.fhirapi

import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import akka.http.scaladsl.model.Uri
import akka.event.{LoggingAdapter,Logging}
import com.databricks.industry.solutions.fhirapi.queries._

import scala.concurrent.ExecutionContext.Implicits.global


class ServiceManager(val qi: QueryInterpreter, val qr: QueryRunner, sqlAlias: Option[BaseAlias] = None) {

  def read(typeSeg: String, idSeg: String)(implicit url: Uri): FormattedOutput = {
    val sql = qi.read(typeSeg, idSeg, url.query().toMap)
    val result = qr.runQuery(QueryInput(sql, url))
    result.error match {
      case Some(x) => FormatManager.ErrorDefault(Seq(result))
      case None => FormatManager.fromResultsNDJson(Seq(result),
        FormatManager.resourcesAsNDJSON, None, sqlAlias)
    }    
  }

  def insert(typeSeg: String, idSeg: String, payload: String)(implicit url: Uri): FormattedOutput = {
    ???
  }

  def getEverything(patientId: String)(implicit url: Uri): FormattedOutput = {
    val queries = qi.readEverythingForPatient(
      patientId,
      {
        qr.runQuery(QueryInput(qi.tablesWithColumns(Seq("subject", "patient", "beneficiary", "individual")))) match {
          case qo if qo.error == None => qo.queryResults.map(row => (row.get("table_name").get, row.get("column_name").get))
          case qo => return FormatManager.ErrorDefault(Seq(qo)) //Error reading metadata tables in UC of tables + columns
        }
      }
    )

    val futureResults = Future.traverse(queries) { query =>
      Future {
        qr.runQuery(QueryInput(query, url))
      }
    }

    val results = Await.result(futureResults, 30.seconds)

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

  def search(typeSeg: String, uri: Map[String, String]): FormattedOutput = { //TODO FormattedOutput with an iterator instead of a list 
    val sql = qi.search(typeSeg, uri) // Builds out the search query to run 
    //run query... 
    //return a paged result (cursor implementation)
    ???
  }

  def allTablesInSchema: Seq[String] = {
    qr.runQuery(QueryInput(qi.allTablesInSchema))
      .queryResults.map(row => row.getOrElse("tableName", ""))
      .filter(_!="")
  }

  def tableColumns(tableName: String): Seq[String] = {
    qr.runQuery(QueryInput(qi.tableSchema(tableName)))
      .queryResults.map(row => row.getOrElse("column_name", ""))
      .filter(_!="")
  }

  def tablesWithColumns(columns: Seq[String]): Seq[(String, String)] = {
    qr.runQuery(QueryInput(qi.tablesWithColumns(columns), Uri("")))
      .queryResults.map(row => (row.getOrElse("table_name", ""), row.getOrElse("column_name", "")))
      .filter( {case(t,_) => t != ""})
  }
}

