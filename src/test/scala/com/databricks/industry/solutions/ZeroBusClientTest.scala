package com.databricks.industry.solutions.fhirapi

import org.scalatest._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Success, Failure}

class ZeroBusClientTest extends BaseTest {
  
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
    
    // Create a sample LogData JSON record with proper protobuf field names (snake_case)
    val sampleLogData = """{
      "query_output": [
        {
          "query_results": [
            {
              "values": {
                "Patient": "{\"resourceType\":\"Patient\",\"id\":\"test-123\"}"
              }
            }
          ],
          "query_runtime": 150,
          "query_start_time": "2024-01-06T10:30:00Z",
          "query_input": "SELECT * FROM patients WHERE id = 'test-123'"
        }
      ],
      "data": "{\"resourceType\":\"Patient\",\"id\":\"test-123\"}",
      "status_cd": "200"
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
    
    // Create a minimal LogData record
    val minimalLogData = """{
      "data": "test data",
      "status_cd": "200"
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
}

