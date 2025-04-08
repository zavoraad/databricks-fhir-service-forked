package com.databricks.industry.solutions.fhirapi

import ca.uhn.fhir.context.FhirContext
import io.github.cdimascio.dotenv.Dotenv

class ServiceManager(val qi: QueryInterpreter, val qr: QueryRunner) {
  private val fhirContext: FhirContext = FhirContext.forR4()
  private val dotenv = Dotenv.configure().ignoreIfMissing().load()
  private val token = dotenv.get("DATABRICKS_TOKEN")

  def read(typeSeg: String, idSeg: String, uri: Map[String, String]): String = {
    val query = qi.read(typeSeg, idSeg, uri)
    println(s"Executing SQL Query: $query")

    val queryOutput = qr.runQuery(QueryInput(query, "user123", token))

    if (queryOutput.queryResults.isEmpty) {
      return """{ "error": "No matching patient found" }"""
    }

    println(s"Returning JSON to Postman: ${queryOutput.queryResults.mkString(",")}")

    s"""
    {
      "resourceType": "Bundle",
      "type": "searchset",
      "entry": [${queryOutput.queryResults.mkString(",")}]
    }
    """
  }
}

object ServiceManager {
  def apply(qi: QueryInterpreter, qr: QueryRunner): ServiceManager = new ServiceManager(qi, qr)
}
