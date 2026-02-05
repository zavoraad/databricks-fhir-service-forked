package com.databricks.industry.solutions.fhirapi

import com.dimafeng.testcontainers.{GenericContainer, ForAllTestContainer}
import org.scalatest.BeforeAndAfterAll
import org.testcontainers.containers.wait.strategy.Wait
import sttp.client3._
import com.databricks.industry.solutions.fhirapi.datastore.PoolDataStore

class DockerIntegrationTest extends BaseTest with ForAllTestContainer with BeforeAndAfterAll {

  // Must be set before Testcontainers initializes Docker client
  System.setProperty("api.version", "1.44")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  // Use the image name defined in build.sbt
  override val container: GenericContainer = GenericContainer(
    dockerImage = "databricks-fhir-api:latest",
    exposedPorts = Seq(9000),
    waitStrategy = Wait.forHttp("/debug/test").forStatusCode(200)
  )

  private def baseUrl: String = s"http://${container.host}:${container.mappedPort(9000)}"

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

    // We don't care if it's 200, 404, or 500 - just that it's NOT 405 (Method Not Allowed)
    assert(response.code.code != 405, s"DELETE returned 405 Method Not Allowed - route is not properly configured")
    // If the route is properly set up, we should get either 200 (if it exists) or 404 (if not found)
    assert(
      response.code.code == 200 || response.code.code == 404 || response.code.code == 500,
      s"Expected 200, 404, or 500 but got ${response.code.code}"
    )
  }

  test("FHIR delete removes Claim by id") {
    insertClaim("-1")
    val backend = HttpClientSyncBackend()
    val request = basicRequest.delete(uri"$baseUrl/fhir/Claim/-1")
    val response = request.send(backend)

    assert(response.code.code == 200)
    response.body match {
      case Right(body) => assert(body.contains("Resource deleted"))
      case Left(error) => fail(s"Unexpected error response: $error")
    }
  }

  test("FHIR delete of missing Claim returns not found") {
    val backend = HttpClientSyncBackend()
    val request = basicRequest.delete(uri"$baseUrl/fhir/Claim/-2")
    val response = request.send(backend)

    assert(response.code.code == 404)
    response.body match {
      case Right(body) => assert(body.contains("Resource not found"))
      case Left(error) => fail(s"Unexpected error response: $error")
    }
  }
}
