package com.databricks.industry.solutions.fhirapi

/*
 Implementing translation from FHIR endpoints
   https://www.hl7.org/fhir/http.html#summary
 
 */
object QueryInterpreter{

  /*
   *
   _format & _pretty do not go into query writing 
   case x if x.contains("_format")
          => params.getOrElse("_format", "json") match {
            //xml, text/xml, application/xml, and application/fhir+xml
            //json, application/json and application/fhir+json
            //ttl, application/fhir+turtle, and text/turtle
          }
   *
   * 
   *  Support select predicates in queries
   */
  def paramsToSelect(params: Map[String, String]): String = {
    params.keySet match {
      case x if x.isEmpty => "*"
      case x if x.contains("_elements") => params.getOrElse("_elements","*")
      case x if x.contains("_summary") =>
        params.getOrElse("_summary", "false") match {
          case "false" => "*"
          case "true" => ???
          case "text" => ???
          case "data" => ???
          case "count" => ???
          case _ => "*"
        }
      case _ => "*"
    }
  }
}


class QueryInterpreter(catalog: String, schema: String){

  //GET [base]/[type]/[id] {?_format=[mime-type]}
  def read(resource: String, id: String, params: Map[String, String]): String = {
    "SELECT " + QueryInterpreter.paramsToSelect(params) +
    " FROM " + catalog + "." + schema + "." +resource +
    " WHERE id = " + id + "\n"
    //" WHERE bundleUUId = '" + id.trim() + "'\n"
  }
}
