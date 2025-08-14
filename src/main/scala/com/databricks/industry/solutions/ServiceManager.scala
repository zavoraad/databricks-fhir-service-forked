package com.databricks.industry.solutions.fhirapi

import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Failure, Success}

class ServiceManager(val qi: QueryInterpreter, val qr: QueryRunner) {

  def read(typeSeg: String, idSeg: String, uri: Map[String, String]): FormattedOutput = {
    val sql = qi.read(typeSeg, idSeg, uri)
    val result = qr.runQuery(QueryInput(sql))
    result.error match {
      case Some(x) => FormatManager.ErrorDefault(result)
      case None =>FormattedOutput(Seq(result), FormatManager.resourceAsNDJSON(result))
    }    
  }

  def insert(typeSeg: String, idSeg: String, uri: Map[String,String], payload: String): FormattedOutput = {
    ???
  }

  def getEverything(patientId: String): FormattedOutput = {
    import scala.concurrent.ExecutionContext.Implicits.global

    val queries = qi.readEverythingForPatient(patientId)

    val futures: Seq[Future[QueryOutput]] = queries.map { query =>
      Future {
        qr.runQuery(QueryInput(query))
      }
    }

    val allResultsFuture: Future[Seq[QueryOutput]] = Future.sequence(futures).map { outputs =>
      outputs
    }

    val results: Seq[QueryOutput] = Await.result(allResultsFuture, 100.seconds)
    //TODO fix this below, need to allow multiple queries 
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
