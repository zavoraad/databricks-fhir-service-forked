package com.databricks.industry.solutions.fhirapi

import akka.http.scaladsl.model.ContentTypes
import akka.actor.ActorSystem
import akka.event.{LoggingAdapter,Logging}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.config.{Config, ConfigFactory}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

import io.circe.syntax._
import io.circe.generic.auto._

//Logging imports
import ch.qos.logback.classic.{Logger, LoggerContext}
import ch.qos.logback.core.ConsoleAppender
import org.slf4j.LoggerFactory
import com.databricks.zerobus.TableProperties
import io.circe.syntax._
import io.circe.generic.auto._ 

import java.time.ZonedDateTime
import io.circe.{Encoder, Json}
import org.joda.time.DateTime
import akka.http.scaladsl.model.StatusCode
import com.google.protobuf.Message
import com.databricks.industry.solutions.fhirapi.queries._
import com.databricks.industry.solutions.fhirapi.datastore.{PoolDataStore, TokenAuth}



trait FhirService {
  implicit val system: ActorSystem
  implicit def executor: ExecutionContext

  def config = ConfigFactory.load()
  val logger: LoggingAdapter


  // Define explicit encoders for types that automatic derivation can't handle directly or correctly
  implicit val jodaDateTimeEncoder: Encoder[DateTime] = Encoder.encodeString.contramap[DateTime](_.toString)
  implicit val statusCodeEncoder: Encoder[StatusCode] = Encoder.encodeInt.contramap[StatusCode](_.intValue())

  lazy val service = {
    ServiceManager(
      QueryInterpreter(config.getString("databricks.data.catalog"), 
      config.getString("databricks.data.schema"),  
      BaseAlias.fromConfig(config, "databricks.alias.sqlpredicate"),
      BaseAlias.fromConfig(config, "databricks.alias.dollareverything")),
      new QueryRunner(
        PoolDataStore(TokenAuth(config.getString("databricks.warehouse.jdbc"), config.getString("databricks.warehouse.token")),
          conRetries = config.getInt("api.jdbc.hikari.connectionRetries"), 
          queryRetries = config.getInt("api.jdbc.hikari.queryRetries"),
          minIdle = config.getInt("api.jdbc.hikari.minIdle"),
          maxPoolSize = config.getInt("api.jdbc.hikari.maxPoolSize"),
          timeoutMS = config.getInt("api.jdbc.hikari.timeoutMS")
          )
      ),
      sqlAlias = Option(BaseAlias.fromConfig(config, "databricks.alias.sqlpredicate").asInstanceOf[BaseAlias])
    )
   }

  val routes: Route = {
    logRequestResult("akka-http-microservice") {
      concat(
        path("debug" / "test") {
          get {
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
                  logger.info(result.asJson.noSpaces)
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
                     logger.info(result.asJson.noSpaces)
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

  // Programmatic Logback Configuration
  val loggerContext = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]  
  
  // Set specific logger levels
  loggerContext.getLogger("com.zaxxer.hikari").setLevel(ch.qos.logback.classic.Level.WARN)
  
  // 2. Start STDOUT logger if configured
  if (config.getString("logging.stdout.enabled") == "true") {
    val consoleAppender = new ConsoleAppender[ch.qos.logback.classic.spi.ILoggingEvent]()
    consoleAppender.setContext(loggerContext)
    consoleAppender.setName("STDOUT")

    // Use a PatternLayoutEncoder with ONLY the message (%msg) and a newline (%n)
    // This prints exactly what you pass to logger.info()
    val encoder = new ch.qos.logback.classic.encoder.PatternLayoutEncoder()
    encoder.setContext(loggerContext)
    encoder.setPattern("%msg%n") 
    encoder.start()

    consoleAppender.setEncoder(encoder)
    consoleAppender.start()
    loggerContext.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).addAppender(consoleAppender)
  }

  // 2. Start zerobus logger if configured
  if (config.getString("logging.zerobus.enabled") == "true") {
     
     
     val zerobusAppender = ZeroBusApiAppender(zclient(config))
   
     zerobusAppender.setContext(loggerContext)
     zerobusAppender.setName("ZEROBUS")
     zerobusAppender.start()
     loggerContext.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).addAppender(zerobusAppender)
  }


  Http().newServerAt(config.getString("http.interface"), config.getInt("http.port")).bindFlow(routes)

  def zclient(config: Config) = new ZeroBusClient(
        config.getString("logging.zerobus.serverEndpoint"),
        config.getString("logging.zerobus.workspaceUrl"),
        config.getString("logging.zerobus.clientId"),
        config.getString("logging.zerobus.clientSecret"),
        config.getString("logging.zerobus.tablename"),
        Class.forName("com.databricks.industry.solutions.fhirapi.Record$" + 
      config.getString("logging.zerobus.tableProtoBuf")).asInstanceOf[Class[? <: Message]]
      )
}



