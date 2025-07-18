package com.databricks.industry.solutions.fhirapi

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import scala.collection.immutable.List
import ujson.Obj

case class FormattedOutput(queryOutput: QueryOutput, bundle: String)

object FormatManager {

    def time(): String = {
        ZonedDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
    }

    def resourcesAsNDJSON(qol: List[QueryOutput], columns: Option[List[String]] = None): String = {
        qol.map(qo => resourceAsNDJSON(qo, columns)).mkString("\n")
    }

    /*
        turn queryResults into ndjson
     */
    def resourceAsNDJSON(qo: QueryOutput, columns: Option[List[String]] = None): String = {
        columns match {
            case Some(c)  => ???
            case None => {
                qo.queryResults.flatMap(t => 
                    t.values.map(x => ujson.read(x))
                ).mkString("\n")
                //.filter(s => s.length > 0).mkString("\n")
            }
        }
    }

    def resourceAsBundle(qol: List[QueryOutput]): Unit = ???
        
    def resourcesAsBundle(qo: QueryOutput, requestType: String = "batch",
    columns: Option[List[String]] = None): String = {
        columns match {
            case Some(c) => ???
            case None => {
                ???
            }
        }
    }


    def fromQueryOutputSearch(queryOutput: QueryOutput): FormattedOutput = {
        FormattedOutput(queryOutput,
        """{"resourceType": "Bundle","type":"searchset","entry":[
        """ +
            queryOutput.queryResults.flatMap(x => {
                x.map { case (key, value) =>
                val j = ujson.read(value)
                j("resourceType") = key 
                Obj("resource" -> j, "fullUrl" -> {"urn:uuid:" + j("fhir_id").value})
            }
            }).mkString(",") +
            """]}"""
        )
    }
}
