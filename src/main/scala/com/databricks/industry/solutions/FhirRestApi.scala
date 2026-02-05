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
import com.databricks.industry.solutions.fhirapi.datastore.{PoolDataStore, Auth, TokenAuth, ServicePrincipalAuth}
import com.databricks.industry.solutions.fhirapi.logging.{ZeroBusApiAppender, ZeroBusClient}



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
        PoolDataStore(FhirService.loadAuth(config),
          conRetries = config.getInt("api.jdbc.hikari.connectionRetries"), 
          queryRetries = config.getInt("api.jdbc.hikari.queryRetries"),
          minIdle = config.getInt("api.jdbc.hikari.minIdle"),
          maxPoolSize = config.getInt("api.jdbc.hikari.maxPoolSize"),
          timeoutMS = config.getInt("api.jdbc.hikari.timeoutMS")
          )
      ),
      sqlAlias = Option(BaseAlias.fromConfig(config, "databricks.alias.sqlpredicate").asInstanceOf[BaseAlias])
    )(executor) // Pass the executor here
   }

  val routes: Route = {
    logRequestResult("akka-http-microservice") {
      concat(
        path("debug" / "testers") {
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
                  onSuccess(service.read(typeSeg, idSeg)(uri)) { result =>
                    complete(result.queryOutput.toString)
                  }
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
                  onSuccess(service.getEverything(patientId)(uri)) { result =>
                  logger.info(result.asJson.noSpaces)
                  complete(result.statusCd, result.data)
                  }
                }
              }
            },
            pathPrefix(Segment) { typeSeg =>
              concat(
                path(Segment) { idSeg =>
                  concat(
                    get {  //read https://build.fhir.org/http.html#read
                      extractUri { uri =>
                        onSuccess(service.read(typeSeg, idSeg)(uri)) { result =>
                          logger.info(result.asJson.noSpaces)
                          complete(result.statusCd, result.data)
                        }
                      }
                    },
                    delete { //delete https://build.fhir.org/http.html#delete
                      // Hard delete example: remove by resource id
                      extractUri { uri => 
                        onSuccess(service.delete(typeSeg, idSeg)(uri)) { result =>
                          logger.info(result.asJson.noSpaces)
                          complete(result.statusCd, result.data)
                        }
                      }
                    }
                  )
                },
                 //create https://build.fhir.org/http.html#create
                post {
                  entity(as[String]) { payload =>
                    extractUri { uri =>
                      onSuccess(service.insert(payload)(uri)) { result =>
                        logger.info(result.asJson.noSpaces)
                        complete(result.statusCd, result.data)
                      }
                    }
                  }
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
  setLogging(config, loggerContext) 

  Http().newServerAt(config.getString("http.interface"), config.getInt("http.port")).bindFlow(routes)

  /* 
  
   */
  def setLogging(config: Config, log: LoggerContext): Unit = {
    // Set specific logger levels
    loggerContext.getLogger("com.zaxxer.hikari").setLevel(ch.qos.logback.classic.Level.WARN)

    // 1. Start STDOUT logger if configured
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
      val zerobusAppender = ZeroBusApiAppender(new ZeroBusClient(
        config.getString("logging.zerobus.serverEndpoint"),
        config.getString("logging.zerobus.workspaceUrl"),
        config.getString("logging.zerobus.clientId"),
        config.getString("logging.zerobus.clientSecret"),
        config.getString("logging.zerobus.tablename"),
        Class.forName("com.databricks.industry.solutions.fhirapi.Record$" + 
      config.getString("logging.zerobus.tableProtoBuf")).asInstanceOf[Class[? <: Message]]
      ))

      zerobusAppender.setContext(loggerContext)
      zerobusAppender.setName("ZEROBUS")
      zerobusAppender.start()
      loggerContext.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).addAppender(zerobusAppender)
    }
  }

  /*
   * Selects an Auth implementation based on config.
   * - databricks.warehouse.auth_cred: "usertoken" or "serviceprincipal"
   * - reads the corresponding config section for credentials
   * - fails fast on unknown values
   */
  def loadAuth(config: Config): Auth = {
    val authType = config.getString("databricks.warehouse.auth_cred").toLowerCase

    authType match {
      case "usertoken" =>
        TokenAuth(
          config.getString("databricks.warehouse.usertoken.auth.jdbc"),
          config.getString("databricks.warehouse.usertoken.auth.token")
        )

      case "serviceprincipal" =>
        ServicePrincipalAuth(
          config.getString("databricks.warehouse.serviceprincipal.auth.jdbc"),
          config.getString("databricks.warehouse.serviceprincipal.auth.http_path"),
          config.getString("databricks.warehouse.serviceprincipal.auth.client_id"),
          config.getString("databricks.warehouse.serviceprincipal.auth.client_secret"),
          config.getString("databricks.warehouse.serviceprincipal.auth.url")
        )

      case other =>
        throw new IllegalArgumentException(s"Unknown auth_cred: $other")
  }
}
}



