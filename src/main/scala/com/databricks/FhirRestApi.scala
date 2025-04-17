package com.databricks.industry.solutions.fhirapi

import akka.http.scaladsl.model.ContentTypes
import akka.actor.ActorSystem
import akka.event.{LoggingAdapter,Logging}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.config.ConfigFactory
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

trait FhirService {
  implicit val system: ActorSystem
  implicit def executor: ExecutionContext

  def config = ConfigFactory.load()
  val logger: LoggingAdapter


  lazy val service = {
    ServiceManager(
      QueryInterpreter(config.getString("databricks.data.catalog"), config.getString("databricks.data.schema")),
      QueryRunner(
        SimpleDataStore(TokenAuth(config.getString("databricks.warehouse.jdbc"), config.getString("databricks.warehouse.token")))
      ))
   }

  val routes: Route = {
    logRequestResult("akka-http-microservice") {
      concat(
        pathPrefix("debug") {
          pathPrefix(Segment){ typeSeg =>
            pathPrefix(Segment) { idSeg =>
              get {
                extractUri { uri =>
                  val result = service.read(typeSeg, idSeg, (uri.query().toMap))
                  complete{ HttpEntity(ContentTypes.`application/json`,result.bundle ) }
                }
              }
            }
          }
        },
        path("debug" / "test") {
          get {
            logger.info("/debug/test endpoint get request made")
            complete(HttpEntity(ContentTypes.`application/json`, """{"status": "FHIR API is running!"}"""))
          }
        },
        pathPrefix("fhir") {
          pathPrefix(Segment){ typeSeg =>
            pathPrefix(Segment) { idSeg =>
              get {
                extractUri { uri =>
                  val result = service.read(typeSeg, idSeg, (uri.query().toMap))
                  complete{ HttpEntity(ContentTypes.`application/json`,result.bundle ) }
                }
              }
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
  override val logger = Logging(system, "AkkaFhirService")
  override val config = ConfigFactory.load()

  Http().newServerAt(config.getString("http.interface"), config.getInt("http.port")).bindFlow(routes)
}
