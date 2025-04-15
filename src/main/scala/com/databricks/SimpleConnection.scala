package com.databricks.industry.solutions.fhirapi

/*
 * One connection reused everywhere
 */
class SimpleConnection(val auth: Auth, val conRetries: Int = 1, val queryRetries: Int = 1) extends Connection{
  override def getConnection: Connection = con
  lazy val con = connect

  override def connect: Connection = {
    def getCon(retries:Int): Connection = {
      try{
        auth.connect
      } catch{
        case r if retries > 0 => getCon(retries -1)
        case t: Exception => throw t
      }
    }
    getCon(conRetries)
  }
}

class QueryRunner (ds: DataStore, queryRetries: Int = 1){

  // method to run the query
  def runQuery(queryInput: QueryInput): QueryOutput = {
    val queryStartTime = DateTime.now
    val r = ds.execute(queryInput.query, queryRetries,ds.getConnection) //returns a tuple of data (Map[String,String], Option(String))
    QueryOutput(r(0), System.currentTimeMillis() - queryStartTime.getMillis, queryStartTime, r(1))

  }
}
