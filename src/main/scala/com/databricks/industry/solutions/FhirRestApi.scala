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
      QueryInterpreter(config.getString("databricks.data.catalog"), 
      config.getString("databricks.data.schema"),  
      BaseAlias.fromConfig(config, "databricks.alias.sqlpredicate"),
      BaseAlias.fromConfig(config, "databricks.alias.dollareverything")),
      new QueryRunner(
        PoolDataStore(TokenAuth(config.getString("databricks.warehouse.jdbc"), config.getString("databricks.warehouse.token")),
          conRetries = 2, queryRetries = 2)
      ),
      sqlAlias = Option(BaseAlias.fromConfig(config, "databricks.alias.sqlpredicate").asInstanceOf[BaseAlias])
    )
   }

  val routes: Route = {
    logRequestResult("akka-http-microservice") {
      concat(
        path("debug" / "test") {
          get {
            logger.info("/debug/test endpoint get request made")
            complete(HttpEntity(ContentTypes.`application/json`, """{"status": "FHIR API is running!"}"""))
          }
        },
        path("debug" / "dbsqlConnect") {
          get {
            complete(service.qr.ds.getConnection.toString)
          }
        },
        pathPrefix("debug") {
          pathPrefix(Segment){ typeSeg =>
            pathPrefix(Segment) { idSeg =>
              get {
                extractUri { uri =>
                  val result = service.read(typeSeg, idSeg)(uri)
                  complete{ result.queryOutput.toString  }
                }
              }
            }
          }
        },
        //main entry point https://build.fhir.org/http.html
        pathPrefix("fhir") {
          concat(
            // Existing logic for /fhir/{resourceType}/{id}/$everything
            pathPrefix("patient" / Segment / "$everything") { patientId =>
              get {
                extractUri { uri =>
                  val result = service.getEverything(patientId)(uri)
                  logger.info(result.info)
                  complete(result.statusCd, result.data)
                }
              }
            },
            pathPrefix(Segment) { typeSeg =>
              concat(
                pathPrefix(Segment) { idSeg => 
                  get {  //read https://build.fhir.org/http.html#read
                   extractUri { uri =>
                     val result = service.read(typeSeg, idSeg)(uri)
                     logger.info(result.info)
                     complete(result.statusCd, result.data)
                    }
                  }
                },
                 //create https://build.fhir.org/http.html#create
                post {

                  ???
                }
            )
            },
            pathPrefix("_search"){
              complete("_search endpoint")
            }
          )
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
