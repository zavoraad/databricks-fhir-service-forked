package com.databricks.industry.solutions.fhirapi

import com.databricks.industry.solutions.fhirapi.queries._
import com.databricks.industry.solutions.fhirapi.datastore._

import org.joda.time.DateTime

class ServiceManagerTest extends BaseTest {

  test("Test Error Handling in get request") {
    val fhirResourceJson = """{"resourceType": "Patient", "id": "1", "name": [{"family": "Test", "given": ["Patient"]}]}"""
    val queryResults = List(QueryResultRow(Map("Patient" -> fhirResourceJson)), QueryResultRow(Map("Patient" -> fhirResourceJson)))
    val qo = QueryOutput(
      queryResults = queryResults,
      queryRuntime = 100,
      queryStartTime = DateTime.now(),
      error = Some("An erorr exists"),
      queryInput = "SELECT * FROM patients",
      url = ""
    )

    val result = FormatManager.resourcesAsNDJSON(List(qo))
    val expected = """{"resourceType":"Patient","id":"1","name":[{"family":"Test","given":["Patient"]}]}
{"resourceType":"Patient","id":"1","name":[{"family":"Test","given":["Patient"]}]}"""
    assert(result == expected)
  }

  test("Test sqlAlias Functions"){
    val fhirResourceJson = """{"resourceType": "Patient", "patientId": "123", "active": true}"""
    val queryResults = List(QueryResultRow(Map("Patient" -> fhirResourceJson)))
    val qo = QueryOutput(
      queryResults = queryResults,
      queryRuntime = 50,
      queryStartTime = DateTime.now(),
      error = None,
      queryInput = "SELECT * FROM patients",
      url = ""
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
    val queryResults = List(QueryResultRow(Map("Patient" -> fhirResourceJson)))
    val qo = QueryOutput(
      queryResults = queryResults,
      queryRuntime = 50,
      queryStartTime = DateTime.now(),
      error = None,
      queryInput = "SELECT * FROM patients",
      url = ""
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
  
  test("Test generateUUID returns valid UUID format") {
    val uuid = ServiceManager.generateUUID
    
    // UUID should match pattern: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    val uuidPattern = """^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$""".r
    assert(uuidPattern.matches(uuid), s"Generated UUID '$uuid' does not match expected format")
  }
  
  test("Test generateMeta returns proper structure") {
    val uuid = "550e8400-e29b-41d4-a716-446655440000"
    val meta = ServiceManager.generateMeta(uuid)
    
    // Verify structure
    assert(meta.obj.contains("versionId"))
    assert(meta.obj.contains("lastUpdated"))
    
    // Verify values
    assert(meta("versionId").str == uuid)
    
    val lastUpdated = meta("lastUpdated").str
    // Verify timestamp format (ISO 8601 with timezone)
    assert(lastUpdated.contains("T"))
    assert(lastUpdated.matches(".*[+-]\\d{2}:\\d{2}$"))
  }
  
  test("Test generateMeta with different UUIDs") {
    val uuid1 = ServiceManager.generateUUID
    val uuid2 = ServiceManager.generateUUID
    
    val meta1 = ServiceManager.generateMeta(uuid1)
    val meta2 = ServiceManager.generateMeta(uuid2)
    
    // Different UUIDs should have different versionIds
    assert(meta1("versionId").str != meta2("versionId").str)
    
    // Both should have timestamps
    assert(meta1.obj.contains("lastUpdated"))
    assert(meta2.obj.contains("lastUpdated"))
  }
}
