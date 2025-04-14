package com.databricks.industry.solutions.fhirapi

import akka.http.scaladsl.model.ContentTypes
import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.config.ConfigFactory
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import io.github.cdimascio.dotenv.Dotenv

trait FhirService {
  implicit val system: ActorSystem
  implicit def executor: ExecutionContext

  def config = ConfigFactory.load()
  val logger: LoggingAdapter

  val qr = new QueryRunner()
  private val dotenv = Dotenv.configure().ignoreIfMissing().load()
  private val token = dotenv.get("DATABRICKS_TOKEN")

  val qi = new QueryInterpreter("hive_metastore", "dbignite_demo_jdbc")
  val sm = new ServiceManager(qi, qr)

  val routes: Route = {
    logRequestResult("akka-http-microservice") {
      concat(
        path("test") {
          get {
            complete(HttpEntity(ContentTypes.`application/json`, """{"status": "FHIR API is running!"}"""))

          }
        },
        path("test-db") {
          get {
            val query = "SELECT * FROM hive_metastore.dbignite_demo_jdbc.patient LIMIT 5"
            val queryInput = QueryInput(query, "admin", token)
            val outputFuture: Future[QueryOutput] = Future(qr.runQuery(queryInput))

            onComplete(outputFuture) {
              case Success(result: QueryOutput) =>
                if (result.queryResults.isEmpty)
                  complete(HttpResponse(StatusCodes.NotFound, entity = "No data found"))
                else
                  complete(HttpEntity(ContentTypes.`application/json`, result.queryResults.mkString(", ")))
              case Failure(ex) =>
                complete(HttpResponse(StatusCodes.InternalServerError, entity = s"""{"error": "Error querying Databricks: ${ex.getMessage}"}"""))
            }
          }
        },
        pathPrefix("fhir") {
          path("Patient" / Segment) { resourceId =>
            get {
              val responseJson = sm.read("Patient", resourceId, Map.empty)
              complete(HttpEntity(ContentTypes.`application/json`, responseJson))
            }
          }
        }
      )
    }
  }
}

object FhirService extends App with FhirService {
  override implicit val system: ActorSystem = ActorSystem()
  override implicit val executor: ExecutionContext = system.dispatcher
  override val logger = system.log

  val serverHost = config.getString("http.interface")
  val serverPort = config.getInt("http.port")
  val port = if (serverPort == 9000) 9001 else serverPort

  Http().newServerAt(serverHost, port).bindFlow(routes)
  println(s"FHIR Service is running at http://$serverHost:$port")
}
