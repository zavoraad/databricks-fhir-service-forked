// package com.databricks.industry.solutions.fhirapi

// object QueryWriter {
//   def paramsToSelect(params: Map[String, String]): String = {
//     params.keySet match {
//       case x if x.isEmpty => "*"
//       case x if x.contains("_elements") => params.getOrElse("_elements", "*")
//       case x if x.contains("_summary") =>
//         params.getOrElse("_summary", "false") match {
//           case "false" => "*"
//           case _ => "*"
//         }
//       case _ => "*"
//     }
//   }
// }

// class QueryWriter(catalog: String, schema: String) {
//   def read(resource: String, id: String, params: Map[String, String]): String = {
//     s"SELECT * FROM $catalog.$schema.$resource WHERE Patient.id = '$id'"
//   }


// }



package com.databricks.industry.solutions.fhirapi

object QueryWriter {
  def paramsToSelect(params: Map[String, String]): String = {
    params.get("_elements").getOrElse("*")
  }
}

class QueryWriter(catalog: String, schema: String) {
  def read(resource: String, id: String, params: Map[String, String]): String = {
    s"""
    SELECT to_json(Patient) AS Patient_JSON
    FROM $catalog.$schema.$resource
    WHERE Patient.id = '$id'
    """.stripMargin
  }
}
