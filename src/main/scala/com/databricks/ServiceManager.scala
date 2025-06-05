package com.databricks.industry.solutions.fhirapi

class ServiceManager(val qi: QueryInterpreter, val qr: QueryRunner) {

  def read(typeSeg: String, idSeg: String, uri: Map[String, String]): FormattedOutput = {
    val sql = qi.read(typeSeg, idSeg, uri)
    val result = qr.runQuery(QueryInput(sql))
    //result
    FormattedOutput.fromQueryOutputSearch(result)
  }

  //TODO Gerta create PaginatedFormattedOutput for iterating through
  def search(typeSeg: String, queryParams: Map[String, String], baseUrl: String): PaginatedFormattedOutput = {
    // Build search query using existing QueryInterpreter
    //val sql = qi.search(typeSeg, queryParams)

    // Execute paginated query
    //val result = qr.executeSearch(
    
    //Format output with pagination links...
    //FormattedOutput.fromPaginatedQueryOutput(result, baseUrl, typeSeg, queryParams)
  }
}
