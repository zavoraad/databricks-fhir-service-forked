package com.databricks.industry.solutions.fhirapi

import org.joda.time.DateTime
import com.databricks.industry.solutions.fhirapi.queries.QueryInterpreter

class QueryInterpreterTest extends BaseTest {
  test("Test Insert SQL Generation"){
    val qi = new QueryInterpreter("catalog", "schema", BaseAlias.empty(), BaseAlias.empty())
    val payload = """{"resourceType": "Patient", "id": "123"}"""
    val sql = qi.insert("Patient", payload)
    val expected = "INSERT INTO catalog.schema.Patient SELECT * FROM (SELECT from_json('{\"resourceType\": \"Patient\", \"id\": \"123\"}', schema_of_json('{\"resourceType\": \"Patient\", \"id\": \"123\"}')))"
    assert(sql == expected)
  }

} 