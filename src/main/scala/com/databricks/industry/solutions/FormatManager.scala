package com.databricks.industry.solutions.fhirapi

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import scala.collection.immutable.List
import ujson.Obj
import akka.http.scaladsl.model.{StatusCode, StatusCodes}

//TODO Update HTTP Response to be the response code applied to the result
case class FormattedOutput(queryOutput: Seq[QueryOutput], data: String, statusCd: StatusCode = StatusCodes.OK)

object FormatManager {

    def ErrorDefault(qo: QueryOutput): FormattedOutput = {
        FormattedOutput(Seq(qo), """{
                "error": "Bad Request",
                "message": """ + qo.error + """",
                "status": 400
            }""""
        )
    }

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
                    t.values.filter(s => s.length > 0).map(x => ujson.read(x))
                ).mkString("\n")
            }
        }
    }

    def resourcesAsBundle(qol: Seq[QueryOutput], columns: Option[List[String]] = None, transactionType: String = "searchset"): String = {
        ujson.write(
            Obj("resourceType" -> "Bundle",
                "type" -> transactionType,
                "entry" -> qol.flatMap(qo => resourcesAsEntry(qo, columns)))
        )
    }
        
    /* 
        This method should only be called by this class as it does not 
        construct the wrapped data needed for a bundle 
     */
    
    def resourcesAsEntry(qo: QueryOutput, columns: Option[List[String]] = None): Seq[Obj] = {
        columns match {
            case Some(c) => ???
            case None => { //builds entry
                qo.queryResults.flatMap(t => 
                    t.values
                        .filter(s => s.length > 0)
                        .map(s => ujson.read(s))
                        .map(j => Obj("resource" -> j, "fullUrl" -> {"urn:uuid:" + j("id").value}))
                )
            }
        }
    }


    def fromQueryOutputSearch(queryOutput: QueryOutput): FormattedOutput = {
        FormattedOutput(Seq(queryOutput),
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
