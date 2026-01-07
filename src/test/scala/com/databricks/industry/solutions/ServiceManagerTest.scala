package com.databricks.industry.solutions.fhirapi

import org.joda.time.DateTime
import com.databricks.industry.solutions.fhirapi.queries.QueryOutput

class ServiceManagerTest extends BaseTest {

  test("Test Error Handling in get request") {
    val fhirResourceJson = """{"resourceType": "Patient", "id": "1", "name": [{"family": "Test", "given": ["Patient"]}]}"""
    val queryResults = List(Map("Patient" -> fhirResourceJson), Map("Patient" -> fhirResourceJson))
    val qo = QueryOutput(
      queryResults = queryResults,
      queryRuntime = 100,
      queryStartTime = DateTime.now(),
      error = Some("An erorr exists"),
      queryInput = "SELECT * FROM patients"
    )

    val result = FormatManager.resourcesAsNDJSON(List(qo))
    val expected = """{"resourceType":"Patient","id":"1","name":[{"family":"Test","given":["Patient"]}]}
{"resourceType":"Patient","id":"1","name":[{"family":"Test","given":["Patient"]}]}"""
    assert(result == expected)
  }

  test("Test sqlAlias Functions"){
    val fhirResourceJson = """{"resourceType": "Patient", "patientId": "123", "active": true}"""
    val queryResults = List(Map("Patient" -> fhirResourceJson))
    val qo = QueryOutput(
      queryResults = queryResults,
      queryRuntime = 50,
      queryStartTime = DateTime.now(),
      error = None,
      queryInput = "SELECT * FROM patients"
    )

    // Create an alias to rename "id" to "patientId"
    val aliasMap = Map("id" -> "patientId")
    val sqlAlias = new BaseAlias(Some(aliasMap))

    // Call the function with the alias
    val result = FormatManager.resourceAsNDJSON(qo, sqlAlias = Some(sqlAlias))

    // Verify that the key has been renamed
    val expected = """{"resourceType":"Patient","active":true,"id":"123"}"""
    assert(result == expected)
  }

  test("Test resourcesAsBundle with sqlAlias") {
    val fhirResourceJson = """{"resourceType": "Patient", "id": "123", "isActive": true}"""
    val queryResults = List(Map("Patient" -> fhirResourceJson))
    val qo = QueryOutput(
      queryResults = queryResults,
      queryRuntime = 50,
      queryStartTime = DateTime.now(),
      error = None,
      queryInput = "SELECT * FROM patients"
    )

    // Create an alias to rename "active" to "isActive"
    val aliasMap = Map("active" -> "isActive")
    val sqlAlias = new BaseAlias(Some(aliasMap))

    // Call the function with the alias
    val resultString = FormatManager.resourcesAsBundle(Seq(qo), sqlAlias = Some(sqlAlias))

    // Parse the result and verify the content
    val resultJson = ujson.read(resultString)
    val resource = resultJson("entry").arr.head("resource")

    assert(resource("resourceType").str == "Patient")
    assert(resource("active").bool == true) // Check for the new aliased key
    assert(!resource.obj.contains("isActive")) // Check that the old key is removed
    assert(resultJson("entry").arr.head("fullUrl").str == "urn:uuid:123")
  }
} 