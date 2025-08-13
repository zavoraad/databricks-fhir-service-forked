package com.databricks.industry.solutions.fhirapi

import org.joda.time.DateTime

class QueryInterpreterTest extends BaseTest {
  test("Test URL Translations"){
    val qi = QueryInterpreter("catalog", "schema")
    val p = qi.readEverythingForPatient("patient123")
  }

} 