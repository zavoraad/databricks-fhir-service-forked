package com.databricks.industry.solutions.fhirapi

import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import akka.http.scaladsl.model.Uri
import akka.event.{LoggingAdapter,Logging}


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
     val results = qi.readEverythingForPatient(patientId).map { query =>
        qr.runQuery(QueryInput(query, url))
      }
      results.filter(qo => qo.error != None) match {
        case l if l.size > 0 => FormatManager.ErrorDefault(results)
        case _ =>  
          FormatManager.fromResultsBundle(results, 
            FormatManager.resourcesAsBundle,
            None,
            "searchset",
            sqlAlias)
      }
    
  }

  def search(typeSeg: String, uri: Map[String, String]): FormattedOutput = { //TODO FormattedOutput with an iterator instead of a list 
    val sql = qi.search(typeSeg, uri) // Builds out the search query to run 
    //run query... 
    //return a paged result (cursor implementation)
    ???
  }
}


/* 
reference: "Patient/de21500ed1025bf65c1b8033ec8ccae8f8f9f29f95f3b2ec2d9b311f60d72ef1"
 */