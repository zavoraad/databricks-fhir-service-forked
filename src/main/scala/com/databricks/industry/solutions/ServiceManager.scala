package com.databricks.industry.solutions.fhirapi

class ServiceManager(val qi: QueryInterpreter, val qr: QueryRunner) {

  def read(typeSeg: String, idSeg: String, uri: Map[String, String]): FormattedOutput = {
    val sql = qi.read(typeSeg, idSeg, uri)
    val result = qr.runQuery(QueryInput(sql))
    //result
    FormattedOutput.fromQueryOutputSearch(result)
  }

  //@Gerta tie the services together of (1) build query, (2) run query, (3) return result paged
  def search(): Unit = {
    ???
  }
}
