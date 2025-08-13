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

  test("Test resourcesAsEntry") {
    val fhirResourceJson = """{"resourceType": "Patient", "id": "1", "name": [{"family": "Test", "given": ["Patient"]}]}"""
    val queryResults = List(Map("Patient" -> fhirResourceJson))
    val qo = QueryOutput(
      queryResults = queryResults,
      queryRuntime = 100,
      queryStartTime = DateTime.now(),
      error = None,
      queryInput = "SELECT * FROM patients WHERE id = '1'"
    )

    val result = FormatManager.resourcesAsEntry(qo)
    val expectedResource = ujson.read(fhirResourceJson)
    val expected = Seq(ujson.Obj("resource" -> expectedResource, "fullUrl" -> "urn:uuid:1"))

    assert(result.length == 1)
    assert(result == expected)
  }

  test("Test entryAsBundle") {
    val fhirResourceJson = """{"resourceType": "Patient", "id": "1", "name": [{"family": "Test", "given": ["Patient"]}]}"""
    val resourceObj = ujson.read(fhirResourceJson)
    val entry = Seq(ujson.Obj("resource" -> resourceObj, "fullUrl" -> "urn:uuid:1"))

    val result = FormatManager.entryAsBundle(entry)
    val resultJson = ujson.read(result)

    assert(resultJson("resourceType").str == "Bundle")
    assert(resultJson("type").str == "batch")
    assert(resultJson("entry").arr.length == 1)
    assert(resultJson("entry")(0) == entry.head)
  }

} 