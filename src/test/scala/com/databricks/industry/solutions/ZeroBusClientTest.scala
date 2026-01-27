package com.databricks.industry.solutions.fhirapi

import org.scalatest._
import ch.qos.logback.classic.{Level, LoggerContext}
import org.slf4j.LoggerFactory
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Success, Failure}
import io.circe.syntax._
import io.circe.generic.auto._
import io.circe.Encoder
import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import org.joda.time.DateTime
import com.databricks.industry.solutions.fhirapi.queries.{QueryOutput, QueryResultRow}

class ZeroBusClientTest extends BaseTest with BeforeAndAfterAll {
  
  // Custom encoders for Joda DateTime and Akka StatusCode for Circe serialization
  implicit val jodaDateTimeEncoder: Encoder[DateTime] = Encoder.encodeString.contramap[DateTime](_.toString)
  implicit val statusCodeEncoder: Encoder[StatusCode] = Encoder.encodeString.contramap[StatusCode](_.intValue().toString)
  
  // Helper method to check if ZeroBus credentials are properly configured
  def hasValidZeroBusConfig: Boolean = {
    config.getString("logging.zerobus.enabled") == "true" &&
    config.getString("logging.zerobus.serverEndpoint").nonEmpty &&
    config.getString("logging.zerobus.workspaceUrl").nonEmpty &&
    config.getString("logging.zerobus.clientId").nonEmpty &&
    config.getString("logging.zerobus.clientSecret").nonEmpty
  }
  
  test("connectivity"){
    // Skip test if ZeroBus is not configured
    assume(config.getString("logging.zerobus.enabled") == "true", 
      "ZeroBus is not enabled in configuration")
    
    val z = zClient
    assert(z != null, "ZeroBusClient should be initialized")
  }

  test("ingest record"){
    // Skip test if ZeroBus is not properly configured with credentials
    assume(hasValidZeroBusConfig, 
      "ZeroBus is not properly configured with valid credentials. " +
      "Set logging.zerobus.enabled=true and provide serverEndpoint, workspaceUrl, clientId, and clientSecret")
    
    val client = zClient
    
    // Create a sample LogData JSON record matching the table structure
    // queryResults: ARRAY<STRUCT<result MAP<STRING, STRING>>>
    // "result" is a native proto3 map, so use JSON object format
    val sampleLogData = """{
      "queryOutput": [
        {
          "queryResults": [
            {
              "result": {
                "Patient": "{\"resourceType\":\"Patient\",\"id\":\"test-123\"}",
                "resourceType": "Patient"
              }
            }
          ],
          "queryRuntime": 150,
          "queryStartTime": "2024-01-06T10:30:00Z",
          "queryInput": "SELECT * FROM patients WHERE id = 'test-123'",
          "url": "http://localhost:9000/fhir/Patient/test-123"
        }
      ],
      "data": "{\"resourceType\":\"Patient\",\"id\":\"test-123\"}",
      "statusCd": "200"
    }"""
    
    // Ingest the record and wait for completion
    try {
      val ingestFuture = client.ingest(sampleLogData)
      
      // Wait for the future to complete (with timeout)
      val result = Await.ready(ingestFuture, 10.seconds)
      
      // Check if the future completed successfully
      result.value.get match {
        case Success(_) => 
          succeed // Test passes if ingest completes without error
        case Failure(exception) => 
          fail(s"Ingest failed with exception: ${exception.getMessage}")
      }
    } catch {
      case e: RuntimeException if e.getMessage.contains("Failed to get Zerobus token") =>
        fail("Authentication failed - check your ZeroBus credentials (clientId, clientSecret, serverEndpoint, workspaceUrl)")
      case e: Exception =>
        fail(s"Unexpected exception during ingest: ${e.getMessage}")
    }
  }

  test("ingest record with minimal data"){
    // Skip test if ZeroBus is not properly configured with credentials
    assume(hasValidZeroBusConfig, 
      "ZeroBus is not properly configured with valid credentials. " +
      "Set logging.zerobus.enabled=true and provide serverEndpoint, workspaceUrl, clientId, and clientSecret")
    
    val client = zClient
    
    // Create a minimal LogData record with camelCase field names
    val minimalLogData = """{
      "data": "test data",
      "statusCd": "200"
    }"""
    
    // Ingest the record
    try {
      val ingestFuture = client.ingest(minimalLogData)
      
      // Wait for completion
      val result = Await.ready(ingestFuture, 10.seconds)
      
      // Verify it completed
      result.value.get match {
        case Success(_) => succeed
        case Failure(exception) => 
          fail(s"Minimal ingest failed: ${exception.getMessage}")
      }
    } catch {
      case e: RuntimeException if e.getMessage.contains("Failed to get Zerobus token") =>
        fail("Authentication failed - check your ZeroBus credentials (clientId, clientSecret, serverEndpoint, workspaceUrl)")
      case e: Exception =>
        fail(s"Unexpected exception during minimal ingest: ${e.getMessage}")
    }
  }

  test("ingest record using FormattedOutput case class"){
    // Skip test if ZeroBus is not properly configured with credentials
    assume(hasValidZeroBusConfig, "ZeroBus is not properly configured")
    
    val client = zClient
    
    // 1. Create the data using case classes (matches table schema exactly now)
    val queryOutput = QueryOutput(
      queryResults = List(QueryResultRow(Map("Patient" -> "{\"resourceType\":\"Patient\",\"id\":\"test-formatted-123\"}"))),
      queryRuntime = 200,
      queryStartTime = DateTime.now(),
      error = None,
      queryInput = "SELECT * FROM patients WHERE id = 'test-formatted-123'",
      url = "http://localhost:9000/fhir/Patient/test-formatted-123"
    )
    
    val formattedOutput = FormattedOutput(
      queryOutput = Seq(queryOutput),
      data = "{\"resourceType\":\"Patient\",\"id\":\"test-formatted-123\"}",
      statusCd = StatusCodes.OK
    )

    // 2. Serialize to JSON (No transformation needed anymore!)
    val jsonString = formattedOutput.asJson.noSpaces

    // 3. Ingest the JSON string
    val ingestFuture = client.ingest(jsonString)
    val result = Await.ready(ingestFuture, 10.seconds)
    
    result.value.get match {
      case Success(_) => succeed
      case Failure(exception) => 
        fail(s"Ingest with FormattedOutput failed: ${exception.getMessage}")
    }
  }
}

