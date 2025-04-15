package com.databricks.industry.solutions.fhirapi

trait Connection{
  val auth: Auth
  val conRetries: Int
  val queryRetries: Int

  def execute(query: String, retries: Int, con: Connection): (List[Map[String, String]], Option[String]) = {
    try{
      val statement = con.createStatement
      val resultSet = statement.executeQuery(query)
      val it = new Iterator[Map[String, String]] {
        def hasNext: Boolean = resultSet.next() // Check if there are more rows
        def next(): Map[String, String] = {
          (1 to resultSet.getMetaData.getColumnCount).map{ i =>
            resultSet.getMetaData.getColumnName(i) -> resultSet.getString(i)
          }.toMap
        }
      }
      (it.toList, None)
    }catch{
      case r if retries > 0 => execute(query, retries-1, con)
      case e: Exception =>
        (List[Map[String, String]](), Some(e.toString))
    }
  }

  protected def connect: Connection //internal class connection handling
  def getConnection: Connection //function to interface with 

  def executePaged(query: String, retries: Int): Unit = {} //todo return iterator for paging
      
}

