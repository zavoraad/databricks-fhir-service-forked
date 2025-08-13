package com.databricks.industry.solutions.fhirapi

import com.typesafe.config.Config
import scala.jdk.CollectionConverters._


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

class QueryInterpreter(val catalog: String, val schema: String, val predicateAlias: Alias) {
  
  def read(resource: String, id: String, params: Map[String, String]): String = {
    "SELECT to_json(struct(" + QueryInterpreter.paramsToSelect(params, resource) + ")) AS " + resource + " FROM " + 
    catalog + "." + schema + "." + resource +
    " WHERE " + predicateAlias.translate("id") + " = '" + id + "'".stripMargin
  }

/* 
e.g. Condition?onset=23.May.2009 => SELECT ... FROM Conidtion Where onset = '23.May.2009'
  Array(params['onset', '23.May.2009'])
 */
  def search(resource: String, params: Map[String, String]): String = {
    ???
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


//Allow aliases on table names or predicates 
//enum Translation: 
//  case WherePredicate, Table, SelectPredicate

trait Alias{
  def translate(s:String): String
  val a: Option[Map[String, String]]
}

class BaseAlias(val a: Option[Map[String, String]] = None) extends Alias{

    override def translate(s: String): String = {
     a match {
      case None => s
      case Some(x) => x.getOrElse(s,s)
    }
  }
}

//Load aliases from a config object
object BaseAlias{
  def fromConfig(config: Config, path: String): Alias = {
    config.hasPath(path) match{
      case true => new BaseAlias(Some(configToMap(config.getConfig(path))))
      case false => BaseAlias(None)
    }
  }

  /* 
    Specific config to a key/value pair of aliases
   */
  def configToMap(config: Config): Map[String, String] = {
    config.root().asScala.map{ case (key, value) => key -> value.unwrapped().toString}.toMap
  }
}

