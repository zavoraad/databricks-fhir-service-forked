import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

class FormatManager(queryOutput: QueryOutput, bundle: String)

object FormatManager {

    def time(): String = {
        ZonedDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
    }

    def resourceAsNDJSON(qol: List(QueryOutput)): ??? = {???}
    def resourcesAsNDJSON(qo: QueryOutput): ??? = {???}

    def resourceAsBundle(qol: List(QueryOutput)): ??? = {???}
    def resourcesAsBundle(qo: QueryOutput): ??? = {???}


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
