package com.databricks.industry.solutions.fhirapi

import com.dimafeng.testcontainers.{GenericContainer, ForAllTestContainer}
import org.scalatest.BeforeAndAfterAll
import org.testcontainers.containers.wait.strategy.Wait
import sttp.client3._
import com.databricks.industry.solutions.fhirapi.datastore.PoolDataStore
import ch.qos.logback.classic.{Level, LoggerContext}
import org.slf4j.LoggerFactory
import ujson._

class DockerIntegrationTest
    extends BaseTest
    with ForAllTestContainer
    with BeforeAndAfterAll {

  // Configure logging before anything else
  private val loggerContext =
    LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]

  // Silence all testcontainers logging
  loggerContext.getLogger("org.testcontainers").setLevel(Level.OFF)
  loggerContext.getLogger("com.github.dockerjava").setLevel(Level.OFF)
  loggerContext.getLogger("tc").setLevel(Level.OFF)

  // Must be set before Testcontainers initializes Docker client
  System.setProperty("api.version", "1.44")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  // Use the image name defined in build.sbt
  override val container: GenericContainer = GenericContainer(
    dockerImage = "databricks-fhir-api:latest",
    exposedPorts = Seq(9000),
    env = Map(
      "jdbc" -> config.getString("databricks.warehouse.usertoken.auth.jdbc"),
      "token" -> config.getString("databricks.warehouse.usertoken.auth.token"),
      "catalog" -> config.getString("databricks.data.catalog"),
      "schema" -> config.getString("databricks.data.schema"),
      "serverEndpoint" -> config.getString("logging.zerobus.serverEndpoint"),
      "workspaceUrl" -> config.getString("logging.zerobus.workspaceUrl"),
      "clientId" -> config.getString("logging.zerobus.clientId"),
      "clientSecret" -> config.getString("logging.zerobus.clientSecret"),
      "serviceprincipal_url" -> "http://dummy.url" // Required but not used in this test
    ),
    waitStrategy = Wait.forHttp("/debug/test").forStatusCode(200)
  )

  private def baseUrl: String =
    s"http://${container.host}:${container.mappedPort(9000)}"

  private def insertClaim(id: String): Unit = {
    val pool = new PoolDataStore(ta)
    try {
      val catalog = config.getString("databricks.data.catalog")
      val schema = config.getString("databricks.data.schema")
      val table = s"$catalog.$schema.Claim"
      val insertSql = s"INSERT INTO $table (id) VALUES ('$id')"
      val con = pool.getConnection
      val stmt = con.createStatement()
      try {
        stmt.executeUpdate(insertSql)
      } finally {
        stmt.close()
        con.close()
      }
    } finally {
      pool.disconnect
    }
  }

  test("Container should start and respond to health check") {
    val backend = HttpClientSyncBackend()
    val request = basicRequest.get(uri"$baseUrl/debug/test")
    val response = request.send(backend)

    assert(response.code.code == 200)
    response.body match {
      case Right(body) => assert(body.contains("FHIR API is running!"))
      case Left(error) => fail(s"Unexpected error response: $error")
    }
  }

  test("DELETE endpoint is reachable and does not return 405") {
    val backend = HttpClientSyncBackend()
    val request = basicRequest.delete(uri"$baseUrl/fhir/Claim/test-route-check")
    val response = request.send(backend)

    val responseBody = response.body match {
      case Right(body) => body
      case Left(error) => error
    }

    // We don't care if it's 200, 204, or 500 - just that it's NOT 405 (Method Not Allowed)
    assert(
      response.code.code != 405,
      s"DELETE returned 405 Method Not Allowed - route is not properly configured. Response: $responseBody"
    )

    // If the route is properly set up, we should get either 200 (if it exists and deleted), 204 (if not found), or 500 (if error)
    assert(
      response.code.code == 200 || response.code.code == 204 || response.code.code == 500,
      s"Expected 200, 204, or 500 but got ${response.code.code}. Response body: $responseBody"
    )
  }

  test("FHIR delete removes Claim by id") {
    insertClaim("-1")
    val backend = HttpClientSyncBackend()
    val request = basicRequest.delete(uri"$baseUrl/fhir/Claim/-1")
    val response = request.send(backend)

    val responseBody = response.body match {
      case Right(body) => body
      case Left(error) => error
    }

    assert(
      response.code.code == 200,
      s"Expected 200 OK but got ${response.code.code}. Response body: $responseBody"
    )
    response.body match {
      case Right(body) =>
        assert(
          body.contains("Resource deleted"),
          s"Response body doesn't contain 'Resource deleted': $body"
        )
      case Left(error) => fail(s"Unexpected error response: $error")
    }
  }

  test("FHIR delete of missing Claim returns 204 No Content") {
    val backend = HttpClientSyncBackend()
    val request = basicRequest.delete(uri"$baseUrl/fhir/Claim/-2")
    val response = request.send(backend)

    val responseBody = response.body match {
      case Right(body) => body
      case Left(error) => error
    }

    // Per FHIR spec: DELETE on a non-existent resource returns 204 No Content
    assert(
      response.code.code == 204,
      s"Expected 204 No Content but got ${response.code.code}. Response body: $responseBody"
    )
    response.body match {
      case Right(body) =>
        assert(
          body.isEmpty,
          s"Expected empty body for 204 No Content but got: $body"
        )
      case Left(error) => fail(s"Unexpected error response: $error")
    }
  }

  test("GET /fhir/metadata returns 200 and CapabilityStatement") {
    val backend = HttpClientSyncBackend()
    val response = basicRequest.get(uri"$baseUrl/fhir/metadata").send(backend)
    assert(
      response.code.code == 200,
      s"Expected 200 OK but got ${response.code.code}"
    )
    response.body match {
      case Right(body) =>
        assert(
          body.contains("CapabilityStatement"),
          s"Expected CapabilityStatement in body: $body"
        )
        assert(
          body.contains("\"resourceType\""),
          s"Expected resourceType in body: $body"
        )
        assert(
          body.contains("\"status\"") && body.contains("active"),
          s"Expected status active: $body"
        )
        assert(body.contains("\"rest\""), s"Expected rest in body: $body")
      case Left(error) => fail(s"Unexpected error response: $error")
    }
  }

  test(
    "GET /fhir/Patient search returns bundle and following next link yields ~1156 patients"
  ) {
    val backend = HttpClientSyncBackend()
    var nextUrl: Option[String] = Some(s"$baseUrl/fhir/Patient")
    var totalEntries = 0
    var pageCount = 0
    val maxPages = 20 // safety: ~100 per page => 20 pages for 2K

    while (nextUrl.nonEmpty && pageCount < maxPages) {
      pageCount += 1
      val url = nextUrl.get
      val request = basicRequest.get(uri"$url")
      val response = request.send(backend)

      assert(
        response.code.code == 200,
        s"Expected 200 OK on page $pageCount (url=$url) but got ${response.code.code}. ${response.body}"
      )
      val body = response.body match {
        case Right(b) => b
        case Left(e)  => fail(s"Request failed: $e")
      }
      val bundle = ujson.read(body).obj
      assert(bundle("resourceType").str == "Bundle", s"Expected Bundle: $body")
      assert(bundle("type").str == "searchset", s"Expected searchset: $body")

      val entries = bundle.get("entry").fold(0)(_.arr.size)
      totalEntries += entries

      nextUrl = bundle
        .get("link")
        .flatMap { linkValue =>
          linkValue.arr
            .find { link =>
              link.obj.get("relation").exists(_.str == "next")
            }
            .flatMap(_.obj.get("url").map(_.str))
        }
        .map { urlStr =>
          if (urlStr.startsWith("http")) urlStr else s"$baseUrl$urlStr"
        }
    }

    assert(
      totalEntries >= 1100 && totalEntries <= 1200,
      s"Expected approximately 1156 Patient resources (1100–1200) but got $totalEntries across $pageCount page(s)"
    )
  }

  // ----- Required FHIR API endpoints not yet implemented (pending until implemented) -----

  test("GET /fhir/_history (history system) returns 501 - Required TODO") {
    pending
  }

  test("GET /fhir/_search (search system) returns 501 - Required TODO") {
    pending
  }

  test("POST /fhir/_search (search system) returns 501 - Required TODO") {
    pending
  }

  test("POST /fhir (batch/transaction) returns 501 - Required TODO") {
    pending
  }

  test(
    "GET /fhir/Patient (search type) - implemented; see test above for search + next link"
  ) {
    // Search and pagination covered by "GET /fhir/Patient search returns bundle and following next link yields ~1156 patients"
  }

  test("PUT /fhir/Patient (conditional update) returns 501 - Required TODO") {
    pending
  }

  test("PATCH /fhir/Patient (conditional patch) returns 501 - Required TODO") {
    pending
  }

  test(
    "DELETE /fhir/Patient (conditional delete) returns 501 - Required TODO"
  ) {
    pending
  }

  test("POST /fhir/Patient (create) returns 501 - Required TODO") {
    pending
  }

  test(
    "GET /fhir/Patient/_history (history type) returns 501 - Required TODO"
  ) {
    pending
  }

  test("GET /fhir/Patient/_search (search type) returns 501 - Required TODO") {
    pending
  }

  test("POST /fhir/Patient/_search (search type) returns 501 - Required TODO") {
    pending
  }

  test(
    "GET /fhir/Patient/test-id/_history (history instance) returns 501 - Required TODO"
  ) {
    pending
  }

  test(
    "DELETE /fhir/Patient/test-id/_history (delete-history) returns 501 - Required TODO"
  ) {
    pending
  }

  test(
    "GET /fhir/Patient/test-id/_history/v1 (vread) returns 501 - Required TODO"
  ) {
    pending
  }

  test(
    "DELETE /fhir/Patient/test-id/_history/v1 (delete-history-version) returns 501 - Required TODO"
  ) {
    pending
  }

  test("PUT /fhir/Patient/test-id (update) returns 501 - Required TODO") {
    pending
  }

  test("PATCH /fhir/Patient/test-id (patch) returns 501 - Required TODO") {
    pending
  }
}
