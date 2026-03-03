package com.databricks.industry.solutions.fhirapi

import com.databricks.industry.solutions.fhirapi.datastore.PoolDataStore

class FHIRFunctionsUnitTest extends BaseTest {
  test("Hard delete removes Claim by id") {
    assume(canConnect, "Not able to connect to a Databricks resource")
    val pool = new PoolDataStore(ta)
    try {
      val catalog = config.getString("databricks.data.catalog")
      val schema = config.getString("databricks.data.schema")
      val table = s"$catalog.$schema.Claim"
      val insertSql = s"INSERT INTO $table (id) VALUES ('-1')"

      val insertCon = pool.getConnection
      val insertStmt = insertCon.createStatement()
      try {
        insertStmt.executeUpdate(insertSql)
      } finally {
        insertStmt.close()
        insertCon.close()
      }

      def countById(): Int = {
        val countCon = pool.getConnection
        val countStmt = countCon.createStatement()
        val rs = countStmt.executeQuery(s"SELECT count(*) as cnt FROM $table WHERE id = '-1'")
        try {
          if (rs.next()) rs.getInt("cnt") else 0
        } finally {
          rs.close()
          countStmt.close()
          countCon.close()
        }
      }

      assert(countById() > 0, "Expected inserted Claim with id = -1")

      val deleteCon = pool.getConnection
      val deleteStmt = deleteCon.createStatement()
      try {
        deleteStmt.executeUpdate(s"DELETE FROM $table WHERE id = '-1'")
      } finally {
        deleteStmt.close()
        deleteCon.close()
      }

      assert(countById() == 0, "Expected Claim with id = -1 to be deleted")
    } finally {
      pool.disconnect
    }
  }
}
