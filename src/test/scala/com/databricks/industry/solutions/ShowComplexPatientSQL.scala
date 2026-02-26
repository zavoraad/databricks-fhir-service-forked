package com.databricks.industry.solutions.fhirapi

import com.databricks.industry.solutions.fhirapi.queries.QueryInterpreter

object ShowComplexPatientSQL extends App {
  val qi = new QueryInterpreter("catalog", "schema", BaseAlias.empty(), BaseAlias.empty())
  val payload = """{
  "resourceType":"Patient",
  "id":"patient-456",
  "active":true,
  "name":[{
    "use":"official",
    "family":"Doe",
    "given":["Jane"]
  }],
  "telecom":[{
    "system":"phone",
    "value":"555-1234",
    "use":"home"
  }],
  "address":[{
    "line":["123 Main St","Apt 4"],
    "city":"Springfield",
    "state":"IL",
    "postalCode":"62701"
  }]
}"""
  
  val sql = qi.insert("Patient", payload)
  
  println("\n" + "="*100)
  println("GENERATED SQL FROM COMPLEX PATIENT STRUCTURE:")
  println("="*100)
  println(sql)
  println("="*100 + "\n")
}
