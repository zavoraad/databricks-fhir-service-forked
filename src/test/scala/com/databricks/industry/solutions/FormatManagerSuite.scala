package com.databricks.industry.solutions.fhirapi

import org.joda.time.DateTime

class FormatManagerSuite extends BaseTest {

  test("Test resourceAsNDJSON") {
    val fhirResourceJson = """{"resourceType": "Patient", "id": "1", "name": [{"family": "Test", "given": ["Patient"]}]}"""
    val queryResults = List(Map("Patient" -> fhirResourceJson), Map("Patient" -> fhirResourceJson))
    val qo = QueryOutput(
      queryResults = queryResults,
      queryRuntime = 100,
      queryStartTime = DateTime.now(),
      error = None,
      queryInput = "SELECT * FROM patients"
    )

    val result = FormatManager.resourcesAsNDJSON(List(qo))
    val expected = """{"resourceType":"Patient","id":"1","name":[{"family":"Test","given":["Patient"]}]}
{"resourceType":"Patient","id":"1","name":[{"family":"Test","given":["Patient"]}]}"""
    assert(result == expected)
  }

  test("Test empty resourceAsNDJSON") {
    val fhirResourceJson = "" 

    val queryResults = List(Map("Patient" -> fhirResourceJson), Map("Patient" -> fhirResourceJson))
    val qo = QueryOutput(
      queryResults = queryResults,
      queryRuntime = 100,
      queryStartTime = DateTime.now(),
      error = None,
      queryInput = "SELECT * FROM patients"
    )

    val result = FormatManager.resourcesAsNDJSON(List(qo))
    val expected = ""
    assert(result == expected)
  }
} 