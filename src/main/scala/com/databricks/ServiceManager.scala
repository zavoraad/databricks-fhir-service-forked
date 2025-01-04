package com.databricks.industry.solutions.fhirapi

class ServiceManager(qi: QueryInterpreter, qr: QueryRunner){
  /*
   * 
   */
  def read(typeSeg: String, idSeg: String, uri: Map[String, String]) : String = {
    //TODO convert to proper string
    qr.runQuery(QueryInput(qi.read(typeSeg, idSeg, uri))).asInstanceOf[String]
  }
}

object ServiceManager{

}
