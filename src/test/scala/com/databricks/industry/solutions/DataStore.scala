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
}
