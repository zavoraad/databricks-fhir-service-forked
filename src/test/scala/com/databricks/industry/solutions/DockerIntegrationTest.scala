package com.databricks.industry.solutions.fhirapi

import com.dimafeng.testcontainers.{GenericContainer, ForAllTestContainer}
import org.scalatest.funsuite.AnyFunSuite
import org.testcontainers.containers.wait.strategy.Wait
import sttp.client3._

class DockerIntegrationTest extends AnyFunSuite with ForAllTestContainer {

  // Use the image name defined in build.sbt
  override val container: GenericContainer = GenericContainer(
    dockerImage = "databricks-fhir-api:latest",
    exposedPorts = Seq(9000),
    waitStrategy = Wait.forHttp("/debug/test").forStatusCode(200)
  )

  test("Container should start and respond to health check") {
    val backend = HttpClientSyncBackend()
    val request = basicRequest.get(uri"http://${container.host}:${container.mappedPort(9000)}/debug/test")
    val response = request.send(backend)

    assert(response.code.code == 200)
    response.body match {
      case Right(body) => assert(body.contains("FHIR API is running!"))
      case Left(error) => fail(s"Unexpected error response: $error")
    }
  }
}
