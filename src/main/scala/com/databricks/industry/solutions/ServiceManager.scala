package com.databricks.industry.solutions.fhirapi

import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import akka.http.scaladsl.model.Uri

class ServiceManager(val qi: QueryInterpreter, val qr: QueryRunner) {

  def read(typeSeg: String, idSeg: String)(implicit url: Uri): FormattedOutput = {
    val sql = qi.read(typeSeg, idSeg, url.query().toMap)
    val result = qr.runQuery(QueryInput(sql, url))
    result.error match {
      case Some(x) => FormatManager.ErrorDefault(result)
      case None =>FormattedOutput(Seq(result), FormatManager.resourceAsNDJSON(result))
    }    
  }

  def insert(typeSeg: String, idSeg: String, payload: String)(implicit url: Uri): FormattedOutput = {
    ???
  }

  def getEverything(patientId: String)(implicit url: Uri): FormattedOutput = {

     val results = qi.readEverythingForPatient(patientId).map { query =>
        qr.runQuery(QueryInput(query, url))
      }

      //TODO fix this below, need to allow multiple queries, allow multiple in parallel
      FormattedOutput(results, FormatManager.resourcesAsBundle(results))
  }

  //@Gerta tie the services together of (1) build query, (2) run query, (3) return result paged

  /* 
      2 primary ways to do paged searches... 
       (1) Run a query with an order by... range 1-100, range 101-200
       (2) Run a query and keep a cursor open

       Search
       (1) Build Query 
        Condition?onset=23.May.2009 => SELECT ... FROM Conidtion Where onset = '23.May.2009'
   */
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