package com.databricks.industry.solutions.fhirapi

class QueryInterpreter(catalog: String, schema: String) {
  def read(resource: String, id: String, params: Map[String, String]): String = {
    s"""
    SELECT to_json(Patient) AS Patient_JSON
    FROM $catalog.$schema.$resource
    WHERE Patient.id = '$id'
    """.stripMargin
  }
}
