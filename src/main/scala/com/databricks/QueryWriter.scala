package com.databricks.industry.solutions.fhirapi

/*
 Implementing translation from FHIR endpoints
   https://www.hl7.org/fhir/http.html#summary
 
 */
// Object (Static Helper)
object QueryWriter{

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

//  Class (Dynamic SQL Generator)
class QueryWriter(catalog: String, schema: String){

  def read(resource: String, id: String, params: Map[String, String]): String = {
    "SELECT " + QueryWriter.paramsToSelect(params) +
    " FROM " + catalog + "." + schema + "." + resource +
    " WHERE bundleUUID = '" + id.trim() + "'\n"
  }
}
