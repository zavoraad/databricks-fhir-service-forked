package com.databricks.industry.solutions.fhirapi

import org.joda.time.DateTime

class QueryInterpreterTest extends BaseTest {
  test("Test URL Translations"){
    val qi = new QueryInterpreter("catalog", "schema", BaseAlias.empty(), BaseAlias.empty())
    //val p = qi.readEverythingForPatient("patient123", Seq("Encounter"))
    // Add assertions here to verify the output of 'p'
    //assert(p.nonEmpty)
  }

} 