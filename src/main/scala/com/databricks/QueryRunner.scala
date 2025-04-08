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
      val root = objectMapper.readTree(json)

      def cleanExtensions(node: JsonNode): Unit = {
        if (node.isObject) {
          val objNode = node.asInstanceOf[ObjectNode]
          val fields = objNode.fieldNames().asScala.toList

          fields.foreach { field =>
            val child = objNode.get(field)

            // If it's an array named "extension", attempt to clean it
            if (field == "extension" && child.isArray) {
              val cleanedArray = objectMapper.createArrayNode()
              for (elem <- child.elements().asScala) {
                if (elem.isTextual) {
                  try {
                    val parsed = objectMapper.readTree(elem.asText())
                    cleanedArray.add(parsed)
                  } catch {
                    case _: Exception => cleanedArray.add(elem)
                  }
                } else {
                  cleanedArray.add(elem)
                }
              }
              objNode.set("extension", cleanedArray)
            } else {
              // Recurse into nested objects and arrays
              if (child.isObject || child.isArray) {
                cleanExtensions(child)
              }
            }
          }
        } else if (node.isArray) {
          node.elements().asScala.foreach(cleanExtensions)
        }
      }

      cleanExtensions(root)

      objectMapper.writeValueAsString(root)
    } catch {
      case e: Exception =>
        println(s"Failed to deep-clean JSON: ${e.getMessage}")
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
