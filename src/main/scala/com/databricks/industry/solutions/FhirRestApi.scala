package com.databricks.industry.solutions.fhirapi

import akka.http.scaladsl.model.ContentTypes
import akka.actor.ActorSystem
import akka.event.{LoggingAdapter, Logging}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.config.{Config, ConfigFactory}
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
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
import com.databricks.industry.solutions.fhirapi.datastore.{
  PoolDataStore,
  Auth,
  TokenAuth,
  ServicePrincipalAuth
}
import com.databricks.industry.solutions.fhirapi.logging.{
  ZeroBusApiAppender,
  ZeroBusClient
}
import akka.pattern.after
import ujson._

trait FhirService {
  implicit val system: ActorSystem
  implicit def executor: ExecutionContext

  def config = ConfigFactory.load()
  val logger: LoggingAdapter

  /** Builds a FHIR R4 CapabilityStatement for GET [base]/metadata. Per the FHIR
    * spec, servers SHALL provide this. Includes:
    *   - resourceType, status, date, kind, fhirVersion (required/recommended)
    *   - format: response formats (e.g. json)
    *   - rest.resource[]: supported resource types and interactions (read,
    *     search-type, create, update, delete)
    */
  private def capabilityStatementJson(): String = {
    val now = java.time.Instant.now().toString
    val resourceTypes =
      Seq("") // TODO
    val resources = resourceTypes.map { t =>
      Obj(
        "type" -> t,
        "interaction" -> Arr(
          Obj("code" -> "read"),
          Obj("code" -> "search-type"),
          Obj("code" -> "create"),
          Obj("code" -> "update"),
          Obj("code" -> "delete")
        )
      )
    }
    ujson.write(
      Obj(
        "resourceType" -> "CapabilityStatement",
        "status" -> "active",
        "date" -> now,
        "kind" -> "instance",
        "fhirVersion" -> "4.0.1", // TODO
        "format" -> Arr("json"),
        "rest" -> Arr(
          Obj(
            "mode" -> "server",
            "resource" -> Arr(resources*)
          )
        )
      )
    )
  }

  // Define explicit encoders for types that automatic derivation can't handle directly or correctly
  implicit val jodaDateTimeEncoder: Encoder[DateTime] =
    Encoder.encodeString.contramap[DateTime](_.toString)
  implicit val statusCodeEncoder: Encoder[StatusCode] =
    Encoder.encodeInt.contramap[StatusCode](_.intValue())

  /** Returns 501 Not Implemented with a JSON body for required FHIR
    * interactions not yet implemented.
    */
  private def notImplementedResponse(todo: String): Route =
    complete(
      StatusCodes.NotImplemented,
      HttpEntity(
        ContentTypes.`application/json`,
        s"""{"error":"Not implemented","todo":"Required TODO: $todo"}"""
      )
    )

  lazy val service = {
    ServiceManager(
      QueryInterpreter(
        config.getString("databricks.data.catalog"),
        config.getString("databricks.data.schema"),
        BaseAlias.fromConfig(config, "databricks.alias.sqlpredicate"),
        BaseAlias.fromConfig(config, "databricks.alias.dollareverything"),
        config.getInt("fhir.paging.defaultPagingSize")
      ),
      new QueryRunner(
        PoolDataStore(
          FhirService.loadAuth(config),
          conRetries = config.getInt("api.jdbc.hikari.connectionRetries"),
          queryRetries = config.getInt("api.jdbc.hikari.queryRetries"),
          minIdle = config.getInt("api.jdbc.hikari.minIdle"),
          maxPoolSize = config.getInt("api.jdbc.hikari.maxPoolSize"),
          timeoutMS = config.getInt("api.jdbc.hikari.timeoutMS")
        )
      ),
      sqlAlias = Option(
        BaseAlias
          .fromConfig(config, "databricks.alias.sqlpredicate")
          .asInstanceOf[BaseAlias]
      )
    )(executor) // Pass the executor here
  }

  val routes: Route = {
    logRequestResult("akka-http-microservice") {
      concat(
        path("debug" / "test") {
          get {
            complete(
              HttpEntity(
                ContentTypes.`application/json`,
                """{"status": "FHIR API is running!"}"""
              )
            )
          }
        },
        path("debug" / "testers") {
          get {
            complete(
              HttpEntity(
                ContentTypes.`application/json`,
                """{"status": "FHIR API is running!"}"""
              )
            )
          }
        },
        path("debug" / "dbsqlConnect") {
          get {
            complete(service.qr.ds.getConnection.toString)
          }
        },
        path("debug" / "zerobus") {
          get {
            try {
              val client = new ZeroBusClient(
                config.getString("logging.zerobus.serverEndpoint"),
                config.getString("logging.zerobus.workspaceUrl"),
                config.getString("logging.zerobus.clientId"),
                config.getString("logging.zerobus.clientSecret"),
                config.getString("logging.zerobus.tablename"),
                Class
                  .forName(
                    "com.databricks.industry.solutions.fhirapi.Record$" +
                      config.getString("logging.zerobus.tableProtoBuf")
                  )
                  .asInstanceOf[Class[? <: Message]]
              )(executor)
              val dummyRecord = FormattedOutput(
                queryOutput = Seq.empty,
                data = ujson.write(
                  Obj(
                    "message" -> ("Debug zerobus test record from FHIR API at " + java.time.Instant
                      .now())
                  )
                ),
                statusCd = StatusCodes.OK
              )
              val ingestFuture = client.ingest(dummyRecord.data)
              val timeoutFuture = after(15.seconds, system.scheduler)(
                Future.failed(
                  new RuntimeException(
                    "ZeroBus ingest timed out after 15 seconds"
                  )
                )
              )(executor)
              val resultWithTimeout = Future.firstCompletedOf(
                Seq(ingestFuture, timeoutFuture)
              )(executor)
              onComplete(resultWithTimeout) {
                case Success(_) =>
                  complete(
                    StatusCodes.OK,
                    HttpEntity(
                      ContentTypes.`application/json`,
                      """{"status":"ok","message":"Test record sent to ZeroBus successfully."}"""
                    )
                  )
                case Failure(ex) =>
                  val causeMsg = Option(ex.getCause)
                    .map(c => c.getMessage)
                    .getOrElse(ex.getMessage)
                  complete(
                    StatusCodes.InternalServerError,
                    HttpEntity(
                      ContentTypes.`application/json`,
                      ujson.write(
                        Obj(
                          "status" -> "error",
                          "message" -> Option(ex.getMessage)
                            .getOrElse(ex.toString),
                          "exceptionType" -> ex.getClass.getSimpleName,
                          "cause" -> (if (ex.getCause != null) causeMsg
                                      else ujson.Null)
                        )
                      )
                    )
                  )
              }
            } catch {
              case ex: Exception =>
                complete(
                  StatusCodes.InternalServerError,
                  HttpEntity(
                    ContentTypes.`application/json`,
                    ujson.write(
                      Obj(
                        "status" -> "error",
                        "message" -> Option(ex.getMessage)
                          .getOrElse(ex.toString),
                        "exceptionType" -> ex.getClass.getSimpleName
                      )
                    )
                  )
                )
            }
          }
        },
        pathPrefix("debug") {
          pathPrefix(Segment) { typeSeg =>
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
        // main entry point https://build.fhir.org/http.html
        pathPrefix("fhir") {
          concat(
            // GET [base]/metadata - CapabilityStatement (implemented)
            path("metadata") {
              get {
                complete(
                  StatusCodes.OK,
                  HttpEntity(
                    ContentTypes.`application/json`,
                    capabilityStatementJson()
                  )
                )
              }
            },
            // Required TODO: history (system) - GET [base]/_history
            path("_history") {
              get {
                notImplementedResponse("history (system) - GET [base]/_history")
              }
            },
            // Required TODO: search (system) - GET [base]?params or POST [base]/_search
            path("_search") {
              concat(
                get {
                  notImplementedResponse("search (system) - GET [base]?params")
                },
                post {
                  notImplementedResponse(
                    "search (system) - POST [base]/_search"
                  )
                }
              )
            },
            // Required TODO: batch/transaction - POST [base] with Bundle.type batch or transaction
            pathEnd {
              post {
                notImplementedResponse("batch/transaction - POST [base]")
              }
            },
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
                // Required TODO: search (type) GET, conditional update PUT, conditional patch PATCH, conditional delete DELETE - [base]/[type]
                pathEnd {
                  concat(
                    get {
                      // "search (type) - GET [base]/[type]?params"
                      extractUri { uri =>
                        onSuccess(service.search(typeSeg)(uri)) { result =>
                          logger.info(result.asJson.noSpaces)
                          complete(result.statusCd, result.data)
                        }
                      }
                    },
                    put {
                      notImplementedResponse(
                        "conditional update - PUT [base]/[type]?params"
                      )
                    },
                    patch {
                      notImplementedResponse(
                        "conditional patch - PATCH [base]/[type]?params"
                      )
                    },
                    delete {
                      notImplementedResponse(
                        "conditional delete - DELETE [base]/[type]?params"
                      )
                    },
                    // Required TODO: create - POST [base]/[type]; conditional create (If-None-Exist) also not implemented
                    post {
                      entity(as[String]) { _ =>
                        extractUri { _ =>
                          notImplementedResponse(
                            "create - POST [base]/[type] (and conditional create with If-None-Exist)"
                          )
                        }
                      }
                    }
                  )
                },
                // Required TODO: history (type) - GET [base]/[type]/_history
                path("_history") {
                  get {
                    notImplementedResponse(
                      "history (type) - GET [base]/[type]/_history"
                    )
                  }
                },
                // Required TODO: search (type) POST - POST [base]/[type]/_search
                path("_search") {
                  concat(
                    get {
                      notImplementedResponse(
                        "search (type) GET - GET [base]/[type]/_search"
                      )
                    },
                    post {
                      notImplementedResponse(
                        "search (type) - POST [base]/[type]/_search"
                      )
                    }
                  )
                },
                path(Segment) { idSeg =>
                  concat(
                    // Required TODO: history (instance) - GET [base]/[type]/[id]/_history
                    path("_history") {
                      concat(
                        get {
                          notImplementedResponse(
                            "history (instance) - GET [base]/[type]/[id]/_history"
                          )
                        },
                        // Required TODO: delete-history - DELETE [base]/[type]/[id]/_history
                        delete {
                          notImplementedResponse(
                            "delete-history - DELETE [base]/[type]/[id]/_history"
                          )
                        }
                      )
                    },
                    // Required TODO: vread - GET [base]/[type]/[id]/_history/[vid]; delete-history-version - DELETE [base]/[type]/[id]/_history/[vid]
                    path("_history" / Segment) { vid =>
                      concat(
                        get {
                          notImplementedResponse(
                            "vread - GET [base]/[type]/[id]/_history/[vid]"
                          )
                        },
                        delete {
                          notImplementedResponse(
                            "delete-history-version - DELETE [base]/[type]/[id]/_history/[vid]"
                          )
                        }
                      )
                    },
                    // read, delete, update, patch on [base]/[type]/[id] (no further path)
                    pathEnd {
                      concat(
                        get { // read https://build.fhir.org/http.html#read
                          extractUri { uri =>
                            onSuccess(service.read(typeSeg, idSeg)(uri)) {
                              result =>
                                logger.info(result.asJson.noSpaces)
                                complete(result.statusCd, result.data)
                            }
                          }
                        },
                        delete { // delete https://build.fhir.org/http.html#delete
                          extractUri { uri =>
                            onSuccess(service.delete(typeSeg, idSeg)(uri)) {
                              result =>
                                logger.info(result.asJson.noSpaces)
                                complete(result.statusCd, result.data)
                            }
                          }
                        },
                        // Required TODO: update - PUT [base]/[type]/[id]
                        put {
                          notImplementedResponse(
                            "update - PUT [base]/[type]/[id]"
                          )
                        },
                        // Required TODO: patch - PATCH [base]/[type]/[id]
                        patch {
                          notImplementedResponse(
                            "patch - PATCH [base]/[type]/[id]"
                          )
                        }
                      )
                    }
                  )
                }
              )
            }
          )
        }
      )
    }
  }
}

object FhirService extends App with FhirService {
  // FHIR REST API Service Entry Point
  override implicit val system: ActorSystem = ActorSystem()
  override implicit val executor: ExecutionContext = system.dispatcher
  override val logger = Logging(system, "AkkaFhirService")
  override val config = ConfigFactory.load()

  // Programmatic Logback Configuration
  val loggerContext =
    LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
  setLogging(config, loggerContext)

  Http()
    .newServerAt(config.getString("http.interface"), config.getInt("http.port"))
    .bindFlow(routes)

  /*
  
   */
  def setLogging(config: Config, log: LoggerContext): Unit = {
    // Set specific logger levels
    loggerContext
      .getLogger("com.zaxxer.hikari")
      .setLevel(ch.qos.logback.classic.Level.WARN)

    // 1. Start STDOUT logger if configured
    if (config.getString("logging.stdout.enabled") == "true") {
      val consoleAppender =
        new ConsoleAppender[ch.qos.logback.classic.spi.ILoggingEvent]()
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
      loggerContext
        .getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)
        .addAppender(consoleAppender)
    }

    // 2. Start zerobus logger if configured
    if (config.getString("logging.zerobus.enabled") == "true") {
      val zerobusAppender = ZeroBusApiAppender(
        new ZeroBusClient(
          config.getString("logging.zerobus.serverEndpoint"),
          config.getString("logging.zerobus.workspaceUrl"),
          config.getString("logging.zerobus.clientId"),
          config.getString("logging.zerobus.clientSecret"),
          config.getString("logging.zerobus.tablename"),
          Class
            .forName(
              "com.databricks.industry.solutions.fhirapi.Record$" +
                config.getString("logging.zerobus.tableProtoBuf")
            )
            .asInstanceOf[Class[? <: Message]]
        )
      )

      zerobusAppender.setContext(loggerContext)
      zerobusAppender.setName("ZEROBUS")
      zerobusAppender.start()
      loggerContext
        .getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)
        .addAppender(zerobusAppender)
    }
  }

  /*
   * Selects an Auth implementation based on config.
   * - databricks.warehouse.auth_cred: "usertoken" or "serviceprincipal"
   * - reads the corresponding config section for credentials
   * - fails fast on unknown values
   */
  def loadAuth(config: Config): Auth = {
    val authType =
      config.getString("databricks.warehouse.auth_cred").toLowerCase

    authType match {
      case "usertoken" =>
        TokenAuth(
          config.getString("databricks.warehouse.usertoken.auth.jdbc"),
          config.getString("databricks.warehouse.usertoken.auth.token")
        )

      case "serviceprincipal" =>
        ServicePrincipalAuth(
          config.getString("databricks.warehouse.serviceprincipal.auth.jdbc"),
          config.getString(
            "databricks.warehouse.serviceprincipal.auth.http_path"
          ),
          config.getString(
            "databricks.warehouse.serviceprincipal.auth.client_id"
          ),
          config.getString(
            "databricks.warehouse.serviceprincipal.auth.client_secret"
          ),
          config.getString("databricks.warehouse.serviceprincipal.auth.url")
        )

      case other =>
        throw new IllegalArgumentException(s"Unknown auth_cred: $other")
    }
  }
}
