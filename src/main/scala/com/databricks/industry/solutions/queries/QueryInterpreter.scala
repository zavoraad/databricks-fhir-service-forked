package com.databricks.industry.solutions.fhirapi.queries

import com.databricks.client.jdbc.internal.fasterxml.jackson.databind.deser.ValueInstantiator.Base
import com.databricks.industry.solutions.fhirapi.{Alias, BaseAlias}

object QueryInterpreter {
  def paramsToSelect(params: Map[String, String], resource: String): String = {
    params.keySet match {
      case x if x.isEmpty => "*, '" + resource + "' as resourceType"
      case x if x.contains("_elements") =>
        params.getOrElse("_elements", "*") // _elements?id,birthDate,name.given
      case x if x.contains("_summary") =>
        params.getOrElse("_summary", "false") match {
          case "false" => "*"
          case _       => "*"
        }
      case _ => "*, '" + resource + "' as resourceType"
    }
  }
}

class QueryInterpreter(
    val catalog: String,
    val schema: String,
    val sqlAlias: Alias = BaseAlias.empty(),
    val dollarEverything: Alias = BaseAlias.empty(),
    val pageSize: Int = 100
) {

  def read(
      resource: String,
      id: String,
      params: Map[String, String]
  ): String = {
    "SELECT to_json(struct(" + QueryInterpreter.paramsToSelect(
      params,
      resource
    ) + ")) AS " + resource + " FROM " +
      catalog + "." + schema + "." + resource +
      " WHERE " + sqlAlias.translate("id") + " = '" + id + "'".stripMargin
  }

  // FHIR delete: hard-delete by id
  def delete(
      resource: String,
      id: String,
      params: Map[String, String]
  ): String = {
    "DELETE FROM " + catalog + "." + schema + "." + resource +
      " WHERE " + sqlAlias.translate("id") + " = '" + id + "'".stripMargin
  }

  /*
e.g. Condition?onset=23.May.2009 => SELECT ... FROM Conidtion Where onset = '23.May.2009'
  Array(params['onset', '23.May.2009'])
   */
  def search(resource: String, params: Map[String, String]): String = {
    "SELECT to_json(struct(" + QueryInterpreter.paramsToSelect(
      params,
      resource
    ) + ")) AS " + resource + " FROM " +
      catalog + "." + schema + "." + resource +
      paramsToWhere(params) + " " + paramsToLimit(params)
  }

  def searchWithArrayFilter(
      resource: String,
      params: Map[String, String]
  ): String = {
    "SELECT to_json(struct(" + QueryInterpreter.paramsToSelect(
      params,
      resource
    ) + ")) AS " + resource + " FROM " +
      catalog + "." + schema + "." + resource +
      paramsToWhereWithArrayFilter(params)
  }

  def paramsToWhereWithArrayFilter(params: Map[String, String]): String = {
    params match {
      case x if x.isEmpty => ""
      case _              =>
        " WHERE " + params
          .map {
            case ("identifier", v) if v.contains("|") =>
              val parts = v.split("\\|")
              val system = parts(0)
              val value = if (parts.length > 1) parts(1) else ""
              // Use Spark SQL filter() function to ensure both system and value match on the same array element
              s"size(filter(${sqlAlias.translate("identifier")}, i -> i.system = '$system' AND i.value = '$value')) > 0"
            case (k, v) => sqlAlias.translate(k) + " = \"" + v + "\""
          }
          .mkString(" AND ")
    }
  }

  /*
   patientId = reference data to use in other resources
   info = (Table, Column) to query and filter for the patient's data
   @returns a sequence of queries to be run
   */
  def readEverythingForPatient(
      patientId: String,
      info: Seq[(String, String)]
  ): Seq[String] = {
    Seq(read("Patient", patientId, Map.empty[String, String])) ++
      info.map((table, column) =>
        search(
          table,
          Map(column -> { dollarEverything.translate("prefix") + patientId })
        )
      )
  }

  /** Recursively converts a ujson.Value to a Databricks SQL expression.
    *
    *   - Objects become named_struct(...)
    *   - Arrays become array(...)
    *   - Primitives are converted to their SQL representation
    *
    * This preserves the nested structure of FHIR resources for proper querying.
    */
  private def jsonToSqlExpression(value: ujson.Value): String = {
    value match {
      case str: ujson.Str =>
        // Escape single quotes in strings
        s"'${str.str.replace("'", "''")}'"

      case num: ujson.Num =>
        num.value.toString

      case bool: ujson.Bool =>
        bool.value.toString

      case arr: ujson.Arr =>
        if (arr.arr.isEmpty) {
          // Empty array
          "array()"
        } else {
          // Recursively process each element
          val elements = arr.arr.map(jsonToSqlExpression).mkString(", ")
          s"array($elements)"
        }

      case obj: ujson.Obj =>
        if (obj.obj.isEmpty) {
          // Empty object
          "named_struct()"
        } else {
          // Recursively build named_struct for nested object
          val structArgs = obj.obj
            .flatMap { case (key, nestedValue) =>
              Seq(s"'$key'", jsonToSqlExpression(nestedValue))
            }
            .mkString(", ")
          s"named_struct($structArgs)"
        }

      case ujson.Null =>
        "NULL"
    }
  }

  def insert(resource: String, payload: String): String = {
    // Parse the JSON payload to extract field names and values
    // Using named_struct recursively to preserve nested FHIR resource structure
    // Reference: https://docs.databricks.com/aws/en/sql/language-manual/functions/named_struct

    val jsonParsed = ujson.read(payload)

    // Build named_struct arguments recursively for the entire resource
    val namedStructArgs = jsonParsed.obj
      .flatMap { case (key, value) =>
        Seq(
          s"'$key'",
          jsonToSqlExpression(value)
        )
      }
      .mkString(", ")

    // Generate INSERT statement using recursive named_struct
    s"""INSERT INTO $catalog.$schema.$resource 
       |SELECT named_struct($namedStructArgs) as resource""".stripMargin
  }

  /*
    build the where clause for any query
    Encounter/?subject=123
      - params Map("subject" -> "123")
   */
  def paramsToWhere(params: Map[String, String]): String = {
    params match {
      case x if x.isEmpty => ""
      case _              =>
        " WHERE " + params
          .map(p =>
            p(0) match {
              case "_page"   => null
              case "last_id" => sqlAlias.translate("id") + " >= '" + p(1) + "'"
              case _         =>
                sqlAlias.translate(p(0)) + filterPrefixToSQL(p(1)) + p(1) + "\""
            }
          )
          .filter(_ != null)
          .mkString(" AND ")
    }
  }

  // https://build.fhir.org/search.html#prefix
  def filterPrefixToSQL(s: String): String = {
    s.take(2) match {
      case "eq" => " = "
      case "ne" => " != "
      case "gt" => " > "
      case "ge" => " >= "
      case "lt" => " < "
      case "le" => " <= "
      case "sa" =>
        (
          " > "
        ) // starts after: resource value starts after parameter value
      case "eb" =>
        (
          " < "
        ) // ends before: resource value ends before parameter value
      case "ap" =>
        " = " // approximately (e.g. 10%); implementation-dependent
      case _ => " = "
    }
  }

  def paramsToLimit(params: Map[String, String]): String = {
    " ORDER BY " + sqlAlias.translate(
      "id"
    ) + " LIMIT " + pageSize + 1 // pull in the next record too but don't display it
  }

  /*
  Used for $everything / readEverythingForPatient
   */
  def allTablesInSchema: String = "show tables in " + catalog + "." + schema

  /*
    Only top level columns shown for this, not the full schema
   */
  def tableSchema(tableName: String): String = """SELECT column_name 
    FROM """ + catalog + """.information_schema.columns
    where table_schema = '""" + schema + """'
    and table_name = '""" + tableName + """'
    """

  /*
    Find all tables that contain the following list of columns in the specified schema
   */
  def tablesWithColumns(columns: Seq[String]): String =
    """SELECT distinct table_name, column_name 
    FROM """ + catalog + """.information_schema.columns
    where table_schema = '""" + schema + """'
      and column_name in (""" + columns
      .map(c => "'" + c + "'")
      .mkString(",") + ")"
}
