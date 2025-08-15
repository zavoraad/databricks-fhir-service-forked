package com.databricks.industry.solutions.fhirapi

import java.sql.Connection
import org.scalatest._


class DataStoreTest extends BaseTest with BeforeAndAfter {
  test("TokenAuth Connectivity"){
    assert(canConnect)
  }

  test("SimpleDataStore Connectivity"){
    assume(canConnect, "Not able to connect to a Databricks resource")
  }

  test("PoolDataStore Connectivity"){
    assume(canConnect, "Not able to connect to a Databricks resource")
  }

  test("Resources are properly released"){
    assume(canConnect, "Not able to connect to a Databricks resource")
    val pool = new PoolDataStore(ta)
    try {
      for (i <- 1 to 100) {
        val (_, error) = pool.execute("SELECT 1", 1, pool.getConnection)
        assert(error.isEmpty, s"Query failed on iteration $i with error: ${error.getOrElse("")}")
      }
    } finally {
      pool.disconnect
    }
  }
}
