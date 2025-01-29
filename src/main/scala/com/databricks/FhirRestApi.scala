package com.databricks.industry.solutions.fhirapi

import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding
import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.typesafe.config.{Config, ConfigFactory}
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport
import io.circe.Decoder.Result
import io.circe.{Decoder, Encoder, HCursor, Json}

import java.io.IOException
import scala.concurrent.{ExecutionContext, Future}
import scala.math._


trait FhirService {
  implicit val system: ActorSystem
  implicit def executor: ExecutionContext

  val config = ConfigFactory.load()
  val logger: LoggingAdapter

  val service = {
    ServiceManager(
      QueryInterpreter(config.getString("databricks.data.catalog"), config.getString("databricks.data.schema")),
      QueryRunner(
        TokenAuth(config.getString("databricks.warehouse.jdbc"), config.getString("databricks.warehouse.token"))
      ))
  }

  val routes: Route = {
    logRequestResult("akka-http-microservice") {
      concat (
        path("test") {
          get {
            complete { "TODO test" }
          }
        },
        path("config"){
          get {
            complete {config.toString} 
          }
        },
        pathPrefix("fhir") {
          pathPrefix(Segment){ typeSeg =>
            pathPrefix(Segment) { idSeg =>
              get {
                extractUri { uri =>
                  //complete{"TODO"}
                  //complete{ service.read(typeSeg, idSeg, (uri.query().toMap)) }
                  complete {"TODO"}
                }
              }
            }
          }
        }
      )
    }
  }
}

object AkkFhirService extends App with FhirService {
  override implicit val system: ActorSystem = ActorSystem()
  override implicit val executor: ExecutionContext = system.dispatcher

  override val config = ConfigFactory.load()
  override val logger = Logging(system, "AkkaFhirService")

  Http().newServerAt(config.getString("http.interface"), config.getInt("http.port")).bindFlow(routes)
}
