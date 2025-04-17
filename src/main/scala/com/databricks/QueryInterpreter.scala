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
    "SELECT to_json(" + resource +".*) AS resultset FROM " + 
    catalog + "." + schema + "." + resource +
    " WHERE id = '" + id + "'".stripMargin
  }
}

