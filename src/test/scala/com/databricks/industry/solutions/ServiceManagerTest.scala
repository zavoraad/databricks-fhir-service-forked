package com.databricks.industry.solutions.fhirapi

import org.joda.time.DateTime
import com.databricks.industry.solutions.fhirapi.queries.{QueryInterpreter, QueryRunner, QueryOutput}
import com.databricks.industry.solutions.fhirapi.datastore.{DataStore, Auth}
import akka.http.scaladsl.model.Uri
import java.sql.Connection

class ServiceManagerTest extends BaseTest {

  test("Test Insert Logic") {
    // 1. Setup a mock data store
    val mockDataStore = new DataStore {
      override val auth: Auth = null
      override val conRetries: Int = 1
      override val queryRetries: Int = 1
      override def getConnection: Connection = null
      override def disconnect: Unit = ()
      override protected def connect: Connection = null
      
      // Track the executed query
      var lastQuery: String = ""
      override def execute(query: String, retries: Int, con: Connection): (List[Map[String, String]], Option[String]) = {
        lastQuery = query
        (Nil, None) // Success with no rows
      }
    }

    val qi = new QueryInterpreter("cat", "schema")
    val qr = new QueryRunner(mockDataStore)
    val sm = new ServiceManager(qi, qr)(global)
    
    // 2. Execute insert
    implicit val uri = Uri("http://test.com")
    val payload = """{"resourceType": "Patient", "id": "123"}"""
    val future = sm.insert(payload)
    val result = Await.result(future, 5.seconds)

    // 3. Verify
    assert(result.statusCd.intValue() == 201)
    assert(mockDataStore.lastQuery.contains("INSERT INTO cat.schema.Patient"))
    assert(mockDataStore.lastQuery.contains("from_json"))
  }

  test("Test Error Handling in get request") {
    val fhirResourceJson = """{"resourceType": "Patient", "id": "1", "name": [{"family": "Test", "given": ["Patient"]}]}"""
    import com.databricks.industry.solutions.fhirapi.queries.QueryResultRow
    val queryResults = List(QueryResultRow(Map("Patient" -> fhirResourceJson)), QueryResultRow(Map("Patient" -> fhirResourceJson)))
    val qo = QueryOutput(
      queryResults = queryResults,
      queryRuntime = 100,
      queryStartTime = DateTime.now(),
      error = Some("An erorr exists"),
      queryInput = "SELECT * FROM patients",
      url = "http://localhost:9000/fhir/Patient"
    )

    val resultFuture = FormatManager.resourcesAsNDJSON(List(qo))
    val result = resultFuture // resourcesAsNDJSON is still synchronous
    val expected = """{"resourceType":"Patient","id":"1","name":[{"family":"Test","given":["Patient"]}]}
{"resourceType":"Patient","id":"1","name":[{"family":"Test","given":["Patient"]}]}"""
    assert(result == expected)
  }

  test("Test sqlAlias Functions"){
    val fhirResourceJson = """{"resourceType": "Patient", "patientId": "123", "active": true}"""
    import com.databricks.industry.solutions.fhirapi.queries.QueryResultRow
    val queryResults = List(QueryResultRow(Map("Patient" -> fhirResourceJson)))
    val qo = QueryOutput(
      queryResults = queryResults,
      queryRuntime = 50,
      queryStartTime = DateTime.now(),
      error = None,
      queryInput = "SELECT * FROM patients",
      url = "http://localhost:9000/fhir/Patient"
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
    import com.databricks.industry.solutions.fhirapi.queries.QueryResultRow
    val queryResults = List(QueryResultRow(Map("Patient" -> fhirResourceJson)))
    val qo = QueryOutput(
      queryResults = queryResults,
      queryRuntime = 50,
      queryStartTime = DateTime.now(),
      error = None,
      queryInput = "SELECT * FROM patients",
      url = "http://localhost:9000/fhir/Patient"
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