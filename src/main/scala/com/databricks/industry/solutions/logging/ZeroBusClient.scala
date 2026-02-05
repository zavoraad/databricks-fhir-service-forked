package com.databricks.industry.solutions.fhirapi.logging

import com.databricks.zerobus._
import java.util.concurrent.CompletableFuture
import scala.concurrent.{ExecutionContext, Future}
import com.google.protobuf.{Message, util}
import com.google.protobuf.util.JsonFormat

class ZeroBusClient(val serverEndpoint: String,
                    val workspaceUrl: String,
                    val clientId: String,
                    val clientSecret: String,
                    val tableName: String,
                    clazz: Class[? <: Message])(implicit val executionContext: ExecutionContext) {
    
  val sdk = ZerobusSdk(serverEndpoint, workspaceUrl)
  //Builders for converting strings to serialized data
  val defaultInstance =
    clazz.getMethod("getDefaultInstance")
      .invoke(null)
      .asInstanceOf[Message]
   

  val table = new TableProperties(tableName, defaultInstance)
  val opts = StreamConfigurationOptions.builder()
            .setMaxInflightRecords(50000)
            .setAckCallback(response =>
                System.out.println("Acknowledged offset: " +
                    response.getDurabilityAckUpToOffset()))
            .build()
  lazy val stream = sdk.createStream(table.asInstanceOf[TableProperties[com.google.protobuf.Message]], clientId, clientSecret, opts).join

  def ingest(record: String): Future[Unit] = Future {
    val builder = defaultInstance.newBuilderForType()
    JsonFormat
      .parser()
      .ignoringUnknownFields()
      .merge(record, builder)
    
    // 3. Set a unique ingestId for deduplication
    val ingestId = java.util.UUID.randomUUID().toString
    val ingestIdField = builder.getDescriptorForType.findFieldByName("uuid_ingest_id")
    if (ingestIdField != null) {
      builder.setField(ingestIdField, ingestId)
    }

    // 4. Ingest the record
    stream.ingestRecord(builder.build())
  }
}



