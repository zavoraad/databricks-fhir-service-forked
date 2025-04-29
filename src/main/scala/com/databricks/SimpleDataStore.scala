package com.databricks.industry.solutions.fhirapi

import java.sql.Connection
/*
 * One connection reused everywhere
 */
class SimpleDataStore(val auth: Auth, val conRetries: Int = 1, val queryRetries: Int = 1) extends DataStore{
  override def getConnection: Connection = con //TODO add check to make sure connection is not closed()
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

  override def disconnect: Unit = con.close
}
/*

 */
