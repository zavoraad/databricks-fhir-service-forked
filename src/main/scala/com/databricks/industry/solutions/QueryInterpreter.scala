package com.databricks.industry.solutions.fhirapi

import com.databricks.client.jdbc.internal.fasterxml.jackson.databind.deser.ValueInstantiator.Base


object QueryInterpreter {
  def paramsToSelect(params: Map[String, String], resource: String): String = {
    params.keySet match {
      case x if x.isEmpty => "*, '" + resource + "' as resourceType"
      case x if x.contains("_elements") => params.getOrElse("_elements", "*") // _elements?id,birthDate,name.given
      case x if x.contains("_summary") =>
        params.getOrElse("_summary", "false") match {
          case "false" => "*"
          case _ => "*"
        }
      case _ => "*, '" + resource + "' as resourceType"
    }
  }
}

class QueryInterpreter(val catalog: String, 
  val schema: String, 
  val sqlAlias: Alias = BaseAlias.empty(),
  val dollarEverything: Alias = BaseAlias.empty()) {
  
  def read(resource: String, id: String, params: Map[String, String]): String = {
    "SELECT to_json(struct(" + QueryInterpreter.paramsToSelect(params, resource) + ")) AS " + resource + " FROM " + 
    catalog + "." + schema + "." + resource +
    " WHERE " + sqlAlias.translate("id") + " = '" + id + "'".stripMargin
  }

/* 
e.g. Condition?onset=23.May.2009 => SELECT ... FROM Conidtion Where onset = '23.May.2009'
  Array(params['onset', '23.May.2009'])
 */
  def search(resource: String, params: Map[String, String]): String = {
    "SELECT to_json(struct(" + QueryInterpreter.paramsToSelect(params, resource) + ")) AS " + resource + " FROM " + 
    catalog + "." + schema + "." + resource + 
    paramsToWhere(params)
  }

  def readEverythingForPatient(patientId: String): Seq[String] = {
    Seq(
      read("Patient", patientId, Map.empty[String,String]) 
      ,search("Encounter", Map("subject" -> {dollarEverything.translate("prefix") + patientId}))
      ,search("Observation", Map("subject" -> {dollarEverything.translate("prefix")  + patientId}))
    )
  }
  /* 
    build the where clause for any query
    Encounter/?subject=123
      - params Map("subject" -> "123")
   */
  def paramsToWhere(params: Map[String, String]): String = {
    params match {
      case x if x.isEmpty => ""
      case _ =>  " WHERE " + params.map(p => sqlAlias.translate(p(0)) + " = \"" + p(1) + "\"").mkString(" AND " )
    }
  }
}
