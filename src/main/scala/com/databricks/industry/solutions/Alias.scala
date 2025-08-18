package com.databricks.industry.solutions.fhirapi

import com.typesafe.config.Config
import scala.jdk.CollectionConverters._
//Allow aliases on table names or predicates 
//enum Translation: 
//  case WherePredicate, Table, SelectPredicate

trait Alias{
  def translate(s:String): String
  val a: Option[Map[String, String]]
  override def toString: String = {
    a match {
      case None => ""
      case Some(x) => x.map(p => p(0) + " = " + p(1)).mkString("\n")
    }
  }
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

  def empty(): Alias = BaseAlias(None)

  /* 
    Specific config to a key/value pair of aliases
   */
  def configToMap(config: Config): Map[String, String] = {
    config.root().asScala.map{ case (key, value) => key -> value.unwrapped().toString}.toMap
  }
}
