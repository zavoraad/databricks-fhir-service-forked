package com.databricks.industry.solutions.fhirapi

class ServiceManager(val qi: QueryInterpreter, val qr: QueryRunner){
  /*
   * 
   */
  def read(typeSeg: String, idSeg: String, uri: Map[String, String]) : QueryOutput = {
    qr.runQuery(QueryInput(qi.read(typeSeg, idSeg, uri)))
  }
}

object ServiceManager{

}
