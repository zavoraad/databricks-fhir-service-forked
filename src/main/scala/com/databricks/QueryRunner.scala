/*
 1. optimized for multiple connections
    -JDBC Pool (monolithic tenancy)
 2. Specific input/output for queries


 6. On Behlf Of... Multi tenancy... 
 */

package com.databricks.industry.solutions.fhirapi

import java.sql.{Connection, DriverManager, ResultSet}
import org.joda.time.DateTime

// QueryInput case class
case class QueryInput(query: String)

// QueryOutput case class
//Runtime in milliseconds
case class QueryOutput(val queryResults: List[Map[String, String]], val queryRuntime: Long,val  queryStartTime: DateTime,val  error: Option[String]){
  override def toString : String = {
    "queryRuntime (in ms): " + queryRuntime +
    "\nqueryStartTime: " + queryStartTime +
    "\nqueryError: " + error.toString +
    "\nnumRows: " + queryResults.length
 }
}

trait Auth{
  def connect: Connection
  def disconnect(c: Connection): Unit = c.close
  def canConnect(c: Connection): Boolean = c.isValid(5)
}

class TokenAuth(val jdbcURL: String, private val token: String) extends Auth {
  def connect: Connection = {
    Class.forName("com.databricks.client.jdbc.Driver")
    DriverManager.getConnection(jdbcURL + ";UID=token;PWD=" + token)
  }
}

trait DataStore{
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

/*
 * One connection reused everywhere
 */
class SimpleDS(val auth: Auth, val conRetries: Int = 1, val queryRetries: Int = 1) extends DataStore{
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
