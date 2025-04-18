package com.databricks.industry.solutions.fhirapi

import java.sql.Connection
import org.joda.time.DateTime
import java.util.{Date, UUID}
import com.fasterxml.jackson.databind.ObjectMapper
import ca.uhn.fhir.context.FhirContext
import ca.uhn.fhir.parser.IParser
import org.hl7.fhir.r4.model.{Bundle, Extension, StringType, Basic}

class QueryRunner(ds: DataStore, queryRetries: Int = 1) {
  def runQuery(queryInput: QueryInput): QueryOutput = {
    val queryStartTime = DateTime.now
    val (results, error) = ds.execute(queryInput.query, queryRetries, ds.getConnection)
    QueryOutput(results, System.currentTimeMillis() - queryStartTime.getMillis, queryStartTime, error, queryInput.query)
  }
}

case class QueryInput(query: String)

case class QueryOutput(
  queryResults: List[Map[String, String]],
  queryRuntime: Long,
  queryStartTime: DateTime,
  error: Option[String],
  queryInput: String
) {
  override def toString: String = {
    s"""queryRuntime (in ms): $queryRuntime
       |queryStartTime: $queryStartTime
       |queryError: ${error.getOrElse("None")}
       |numRows: ${queryResults.length}
       |queryExecuted: $queryInput
       |""".stripMargin
  }
}


// can be moved to a new file
case class FormattedOutput(queryOutput: QueryOutput, bundle: String)

object FormattedOutput {
  private val fhirContext: FhirContext = FhirContext.forR4()
  private val parser: IParser = fhirContext.newJsonParser()
  private val objectMapper = new ObjectMapper()

  def fromQueryOutputSearch(queryOutput: QueryOutput): FormattedOutput = {
    val bundle = new Bundle()
    bundle.setType(Bundle.BundleType.SEARCHSET)
    bundle.setId(UUID.randomUUID().toString)
    bundle.setTimestamp(new Date())

    if (queryOutput.queryResults.nonEmpty) {
      queryOutput.queryResults.foreach { row =>
        row.foreach {
          case (_, rawJson) => parseAndAddToBundle(rawJson, bundle)
        }
      }
    }

    bundle.setTotal(bundle.getEntry.size())
    FormattedOutput(queryOutput, parser.encodeResourceToString(bundle))
  }

  private def parseAndAddToBundle(rawJson: String, bundle: Bundle): Unit = {
    try {
      val resource = new Basic()
      val uuid = UUID.randomUUID().toString
      resource.setId(uuid)

      val extension = new Extension()
      extension.setUrl("http://example.org/fhir/StructureDefinition/raw-patient-json")
      extension.setValue(new StringType(rawJson))

      resource.addExtension(extension)

      val entry = new Bundle.BundleEntryComponent()
      entry.setFullUrl(s"urn:uuid:$uuid")
      entry.setResource(resource)
      bundle.addEntry(entry)
    } catch {
      case e: Exception =>
        println("======= Failed to wrap raw JSON in bundle =======")
        println(e.getMessage)
        e.printStackTrace()
    }
  }
}

