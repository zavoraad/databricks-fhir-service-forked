package com.databricks.industry.solutions.fhirapi.queries

import com.databricks.client.jdbc.internal.fasterxml.jackson.databind.deser.ValueInstantiator.Base
import com.databricks.industry.solutions.fhirapi.{Alias, BaseAlias}


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

  // FHIR delete: hard-delete by id
  def delete(resource: String, id: String, params: Map[String, String]): String = {
    "DELETE FROM " + catalog + "." + schema + "." + resource +
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

  def searchWithArrayFilter(resource: String, params: Map[String, String]): String = {
    "SELECT to_json(struct(" + QueryInterpreter.paramsToSelect(params, resource) + ")) AS " + resource + " FROM " + 
    catalog + "." + schema + "." + resource + 
    paramsToWhereWithArrayFilter(params)
  }

  def paramsToWhereWithArrayFilter(params: Map[String, String]): String = {
    params match {
      case x if x.isEmpty => ""
      case _ =>  " WHERE " + params.map {
        case ("identifier", v) if v.contains("|") =>
          val parts = v.split("\\|")
          val system = parts(0)
          val value = if (parts.length > 1) parts(1) else ""
          // Use Spark SQL filter() function to ensure both system and value match on the same array element
          s"size(filter(${sqlAlias.translate("identifier")}, i -> i.system = '$system' AND i.value = '$value')) > 0"
        case (k, v) => sqlAlias.translate(k) + " = \"" + v + "\""
      }.mkString(" AND " )
    }
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

  def insert(resource: String, payload: String): String = {
    // Generate an INSERT statement that parses the JSON payload into the table structure
    // Using from_json with schema_of_json to dynamically map JSON to table schema
    s"INSERT INTO $catalog.$schema.$resource SELECT * FROM (SELECT from_json('$payload', schema_of_json('$payload')))"
    /* 
    INSERT INTO RESOURCE
      k,v
      named_struct (k) -> (v)
     */
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
