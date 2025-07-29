package com.databricks.industry.solutions.fhirapi

class ServiceManager(val qi: QueryInterpreter, val qr: QueryRunner) {

  def read(typeSeg: String, idSeg: String, uri: Map[String, String]): FormattedOutput = {
    val sql = qi.read(typeSeg, idSeg, uri)
    val result = qr.runQuery(QueryInput(sql))
    result.error match {
      case Some(x) => FormatManager.ErrorDefault(result)
      case None =>FormattedOutput(result, FormatManager.resourceAsNDJSON(result))
    }    
  }

  //@Gerta tie the services together of (1) build query, (2) run query, (3) return result paged

  /* 
      2 primary ways to do paged searches... 
       (1) Run a query with an order by... range 1-100, range 101-200
       (2) Run a query and keep a cursor open

       Search
       (1) Build Query 
        Condition?onset=23.May.2009 => SELECT ... FROM Conidtion Where onset = '23.May.2009'
   */
  def search(typeSeg: String, uri: Map[String, String]): FormattedOutput = { //TODO FormattedOutput with an iterator instead of a list 
    val sql = qi.search(typeSeg, uri) // Builds out the search query to run 
    //run query... 
    //return a paged result (cursor implementation)
    ???
  }

  
}
