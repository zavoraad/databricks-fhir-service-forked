package com.databricks.industry.solutions.fhirapi

import com.typesafe.config.Config
import scala.jdk.CollectionConverters._


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

class QueryInterpreter(catalog: String, schema: String, predicateAlias: Alias) {
  
  def read(resource: String, id: String, params: Map[String, String]): String = {
    "SELECT to_json(struct(*)) AS " + resource + " FROM " + 
    catalog + "." + schema + "." + resource +
    " WHERE " + predicateAlias.translate(id) + " = '" + id + "'".stripMargin
  }
}


//Allow aliases on table names or predicates 
//enum Translation: 
//  case WherePredicate, Table, SelectPredicate

trait Alias{
  def translate(s:String): String
}

class BaseAlias(val a: Option[Map[String, String]] = None) extends Alias{

    override def translate(s: String): String = {
     a match {
      case None => s
      case Some(x) => x(s)
    }
  }
}

//Load aliases from a config object
object BaseAlias{
  def fromConfig(config: Config, path: String): Alias = {
    config.hasPath(path) match{
      case true => new BaseAlias(Some(configToMap(config.atPath(path))))
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

