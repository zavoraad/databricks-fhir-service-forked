
package com.databricks.industry.solutions.fhirapi
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Failure, Success}


class ServiceManager(val qi: QueryInterpreter, val qr: QueryRunner) {

  def read(resource: String, id: String, params: Map[String, String]): FormattedOutput = {
    val query = qi.read(resource, id, params)
    val result = qr.runQuery(QueryInput(query))
    FormattedOutput.fromQueryOutputSearch(result)
  }

  def getEverything(patientId: String): FormattedOutput = {
    import scala.concurrent.ExecutionContext.Implicits.global

    val queries = qi.readEverythingForPatient(patientId)

    val futures: Seq[Future[QueryOutput]] = queries.map { query =>
      Future {
        qr.runQuery(QueryInput(query))
      }
    }

    val allResultsFuture: Future[List[Map[String, String]]] = Future.sequence(futures).map { outputs =>
      outputs.flatMap(_.queryResults).toList
    }

    val results: List[Map[String, String]] = Await.result(allResultsFuture, 10.seconds)

    val combinedOutput = QueryOutput(
      queryResults = results,
      queryRuntime = 0L,
      queryStartTime = org.joda.time.DateTime.now(),
      error = None,
      queryInput = "Multiple queries for $everything"
    )

    FormattedOutput.fromQueryOutputSearch(combinedOutput)
  }



}
