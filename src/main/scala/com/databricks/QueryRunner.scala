package com.databricks.industry.solutions.fhirapi

import ca.uhn.fhir.context.FhirContext
import java.sql.{Connection, ResultSet}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.{ObjectNode, ArrayNode}
import scala.jdk.CollectionConverters._

case class QueryInput(query: String, oboUser: String, userToken: String)
case class QueryOutput(queryResults: List[String], queryRuntime: Long)

class QueryRunner {
  private val fhirContext: FhirContext = FhirContext.forR4()
  private val objectMapper = new ObjectMapper()

  def runQuery(queryInput: QueryInput): QueryOutput = {
    val startTime = System.currentTimeMillis()
    val dbResult = runDatabaseQuery(queryInput.query, queryInput)
    QueryOutput(dbResult, System.currentTimeMillis() - startTime)
  }

  private def runDatabaseQuery(query: String, queryInput: QueryInput): List[String] = {
    val connection: Connection = DatabaseConnectionPool.getConnection(queryInput.oboUser, queryInput.userToken)
    var resultSet: ResultSet = null
    val resultsList = scala.collection.mutable.ListBuffer[String]()

    try {
      val statement = connection.createStatement()
      println(s"Executing SQL Query: $query")
      resultSet = statement.executeQuery(query)

      while (resultSet.next()) {
        val rawJson = resultSet.getString("Patient_JSON")
        val cleanedJson = cleanJson(rawJson)
        val enrichedJson = addResourceType(cleanedJson, "Patient")
        resultsList += s"""{ "resource": $enrichedJson }"""
      }
    } catch {
      case e: Exception =>
        println(s"Error executing query: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      if (resultSet != null) resultSet.close()
      if (connection != null) connection.close()
    }

    resultsList.toList
  }

  private def cleanJson(json: String): String = {
    try {
      val parsedJson: JsonNode = objectMapper.readTree(json)
      if (parsedJson.has("extension") && parsedJson.get("extension").isArray) {
        val extArray = parsedJson.get("extension")
        val fixedExtensions: ArrayNode = objectMapper.createArrayNode()
        for (ext <- extArray.elements().asScala) {
          if (ext.isTextual) {
            try fixedExtensions.add(objectMapper.readTree(ext.asText()))
            catch { case _: Exception => fixedExtensions.add(ext) }
          } else if (ext.isObject) {
            fixedExtensions.add(ext)
          }
        }
        val objNode = parsedJson.deepCopy[ObjectNode]()
        objNode.set("extension", fixedExtensions)
        return objectMapper.writeValueAsString(objNode)
      }
      objectMapper.writeValueAsString(parsedJson)
    } catch {
      case e: Exception =>
        println(s"Failed to clean JSON: ${e.getMessage}")
        json
    }
  }

  private def addResourceType(json: String, resourceType: String): String = {
    try {
      val jsonNode = objectMapper.readTree(json)
      if (!jsonNode.has("resourceType") && jsonNode.isObject) {
        val objNode = jsonNode.deepCopy[ObjectNode]()
        objNode.put("resourceType", resourceType)
        return objectMapper.writeValueAsString(objNode)
      }
    } catch {
      case e: Exception =>
        println(s"Failed to add resourceType: ${e.getMessage}")
    }
    json
  }
}
