package com.databricks.industry.solutions.fhirapi

import java.sql.{Connection, DriverManager, ResultSet}
import org.joda.time.DateTime
import java.util.{Date, UUID}

class QueryRunner (ds: DataStore, queryRetries: Int = 1){

  // method to run the query
  def runQuery(queryInput: QueryInput): QueryOutput = {
    val queryStartTime = DateTime.now
    val r = ds.execute(queryInput.query, queryRetries,ds.getConnection) //returns a tuple of data (Map[String,String], Option(String))
    QueryOutput(r(0), System.currentTimeMillis() - queryStartTime.getMillis, queryStartTime, r(1), queryInput.query)

  }
}

case class QueryInput(query: String)

case class QueryOutput(val queryResults: List[Map[String, String]], val queryRuntime: Long,val  queryStartTime: DateTime,val  error: Option[String], queryInput: String){
  override def toString : String = {
    "queryRuntime (in ms): " + queryRuntime +
    "\nqueryStartTime: " + queryStartTime +
    "\nqueryError: " + error.toString +
    "\nnumRows: " + queryResults.length + 
    "\nqueryExecuted: " + queryInput
 }
}

case class FormattedOutput(queryOutput: QueryOutput, bundle: String)


/*
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
 }
 */
