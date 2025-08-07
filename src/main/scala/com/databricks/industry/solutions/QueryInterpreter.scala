package com.databricks.industry.solutions.fhirapi

object QueryInterpreter {
  def paramsToSelect(params: Map[String, String]): String = {
    params.keySet match {
      case x if x.isEmpty => "*"
      case x if x.contains("_elements") => params.getOrElse("_elements", "*")
      case x if x.contains("_summary") =>
        params.getOrElse("_summary", "false") match {
          case "false" => "*"
          case _ => "*"
        }
      case _ => "*"
    }
  }
}
class QueryInterpreter(catalog: String, schema: String) {
  def read(resource: String, id: String, params: Map[String, String]): String = {
    "SELECT to_json(struct(*)) AS " + resource + " FROM " + 
    catalog + "." + schema + "." + resource +
    " WHERE fhir_id = '" + id + "'".stripMargin
  }
  def readEverythingForPatient(patientId: String): Seq[String] = {
    val ref = s"'Patient/$patientId'"
    Seq(
      s"SELECT to_json(struct(*)) AS Patient FROM $catalog.$schema.Patient WHERE fhir_id = '$patientId'",
      s"SELECT to_json(struct(*)) AS Encounter FROM $catalog.$schema.Encounter WHERE subject.reference = $ref",
      s"SELECT to_json(struct(*)) AS Observation FROM $catalog.$schema.Observation WHERE subject.reference = $ref"
    )
  }
}

