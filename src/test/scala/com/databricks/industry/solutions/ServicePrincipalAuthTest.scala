package com.databricks.industry.solutions.fhirapi

import org.scalatest.funsuite.AnyFunSuite
import org.joda.time.DateTime
import com.databricks.industry.solutions.fhirapi.datastore.ServicePrincipalAuth

class ServicePrincipalAuthTest extends BaseTest {
  val auth = new ServicePrincipalAuth(
    "jdbc:databricks://example.com:443",
    "/sql/1.0/warehouses/test",
    "client-id",
    "client-secret",
    "https://example.com/oidc/v1/token"
  )

  test("SLToken expiresWithin returns true near expiry") {
    val token = new auth.SLToken("fake-token", DateTime.now().plusSeconds(30))
    assert(token.expiresWithin(60))
  }

  test("SLToken expiresWithin returns false for far future") {
    val token = new auth.SLToken("fake-token", DateTime.now().plusSeconds(600))
    assert(!token.expiresWithin(60))
  }
}
