package com.databricks.industry.solutions.fhirapi

import ca.uhn.fhir.context.FhirContext
import ca.uhn.fhir.parser.IParser
import org.hl7.fhir.r4.model.{Patient, Bundle}
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.{ObjectNode, ArrayNode}

import java.sql.{Connection, ResultSet}
import java.util.{Date, UUID}
import scala.jdk.CollectionConverters._

case class QueryInput(query: String, oboUser: String, userToken: String)
case class QueryOutput(bundle: Bundle, queryRuntime: Long)

class QueryRunner {
  private val fhirContext: FhirContext = FhirContext.forR4()
  private val parser: IParser = fhirContext.newJsonParser()
  private val objectMapper = new ObjectMapper()

  def runQuery(queryInput: QueryInput): QueryOutput = {
    val startTime = System.currentTimeMillis()
    val bundle = runDatabaseQuery(queryInput.query, queryInput)
    QueryOutput(bundle, System.currentTimeMillis() - startTime)
  }

  private def runDatabaseQuery(query: String, queryInput: QueryInput): Bundle = {
    val connection: Connection = DatabaseConnectionPool.getConnection(queryInput.oboUser, queryInput.userToken)
    var resultSet: ResultSet = null

    val bundle = new Bundle()
    bundle.setType(Bundle.BundleType.SEARCHSET)
    bundle.setId(UUID.randomUUID().toString)
    bundle.setTimestamp(new Date())

    try {
      val statement = connection.createStatement()
      println(s"Executing SQL Query: $query")
      resultSet = statement.executeQuery(query)

      while (resultSet.next()) {
        val rawJson = resultSet.getString("Patient_JSON")

        println("🧪 Raw JSON from DB:")
        println(rawJson)

        try {
          val jsonNode = objectMapper.readTree(rawJson).asInstanceOf[ObjectNode]

          // Ensure "resourceType" is present
          if (!jsonNode.has("resourceType")) {
            println("⚠️ JSON missing resourceType – patching as 'Patient'")
            jsonNode.put("resourceType", "Patient")
          }

          // Recursively clean all stringified extensions
          fixStringifiedExtensions(jsonNode)

          val cleanedJson = objectMapper.writeValueAsString(jsonNode)

          println("🧼 Cleaned JSON with extensions fixed:")
          println(cleanedJson)

          // Only parse if valid
          if (cleanedJson.contains("\"resourceType\"")) {
            val patient = parser.parseResource(classOf[Patient], cleanedJson)

            val entry = new BundleEntryComponent()
            entry.setFullUrl(s"urn:uuid:${patient.getIdElement.getIdPart}")
            entry.setResource(patient)
            bundle.addEntry(entry)
          } else {
            println("❌ Skipping resource – still missing 'resourceType' after cleaning.")
          }

        } catch {
          case e: Exception =>
            println(s"❌ Error parsing Patient JSON: ${e.getMessage}")
            e.printStackTrace()
        }
      }
    } catch {
      case e: Exception =>
        println(s"Error executing query: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      if (resultSet != null) resultSet.close()
      if (connection != null) connection.close()
    }

    bundle.setTotal(bundle.getEntry.size())
    bundle
  }

  // Recursively fixes any stringified extension field in any object
  private def fixStringifiedExtensions(node: JsonNode): Unit = {
    node match {
      case objNode: ObjectNode =>
        val fields = objNode.fieldNames().asScala.toList
        fields.foreach { field =>
          val child = objNode.get(field)

          if (field == "extension" && child.isArray) {
            val cleanedArray = objectMapper.createArrayNode()
            for (elem <- child.elements().asScala) {
              if (elem.isTextual) {
                try {
                  val parsed = objectMapper.readTree(elem.asText())
                  fixStringifiedExtensions(parsed)
                  cleanedArray.add(parsed)
                } catch {
                  case _: Exception => cleanedArray.add(elem)
                }
              } else {
                fixStringifiedExtensions(elem)
                cleanedArray.add(elem)
              }
            }
            objNode.set("extension", cleanedArray)
          } else {
            fixStringifiedExtensions(child)
          }
        }

      case arrayNode: ArrayNode =>
        arrayNode.elements().asScala.foreach(fixStringifiedExtensions)

      case _ => // do nothing
    }
  }
}
