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

  // URL -> /<resource>/<id> -> SELECT to_json(struct(*)) from <resource> where fhir_id = "ID"
  def read(resource: String, id: String, params: Map[String, String]): String = {
    "SELECT to_json(struct(*)) AS " + resource + " FROM " + 
    catalog + "." + schema + "." + resource +
    " WHERE fhir_id = '" + id + "'".stripMargin
  }

  // URL search/patient/?dateOfBirth=11/14?... -> /<resource>/ -> SELECT to_json(struct(*)) from <resource> where dateOfBirth = '11/14'
  //@Gerta TODO
  def search(resource: String, params: Map[String, String]): String = {
    ???
  }
}

