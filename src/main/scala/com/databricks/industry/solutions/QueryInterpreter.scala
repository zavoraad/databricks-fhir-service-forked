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

 /* 
   patientId = reference data to use in other resources
   info = (Table, Column) to query and filter for the patient's data 
   @returns a sequence of queries to be run 
  */
  def readEverythingForPatient(patientId: String, info: Seq[(String,String)]): Seq[String] = {
    Seq(read("Patient", patientId, Map.empty[String,String]) ) ++ 
      info.map((table, column) => 
        search(table, Map(column -> {dollarEverything.translate("prefix") + patientId})))
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

/* 
  Used for $everything / readEverythingForPatient
 */
  def allTablesInSchema:String = "show tables in "  + catalog + "." + schema
  
  /* 
    Only top level columns shown for this, not the full schema
   */
  def tableSchema(tableName: String):String = """SELECT column_name 
    FROM """ + catalog + """.information_schema.columns
    where table_schema = '""" + schema + """'
    and table_name = '""" + tableName + """'
    """

  /* 
    Find all tables that contain the following list of columns in the specified schema
   */
  def tablesWithColumns(columns: Seq[String]):String = """SELECT distinct table_name, column_name 
    FROM """ + catalog + """.information_schema.columns
    where table_schema = '""" + schema + """'
      and column_name in (""" + columns.map(c => "'" + c + "'").mkString(",") +  ")"
}
