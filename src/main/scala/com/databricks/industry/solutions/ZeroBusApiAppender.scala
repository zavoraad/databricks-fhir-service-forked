package com.databricks.industry.solutions.fhirapi

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.UnsynchronizedAppenderBase
import java.net.URI
import java.time.Duration
import io.circe.syntax._
import io.circe.generic.auto._ 

class ZeroBusApiAppender (val client: ZeroBusClient) extends UnsynchronizedAppenderBase[ILoggingEvent] {

  override def append(event: ILoggingEvent): Unit = {

    // 3. Send (Fire and Forget)
    try {
      System.out.println("It's shappening!!!")
      client.ingest(event.getFormattedMessage)
    } catch {
      case e: Exception => 
        // CAUTION: Do not log here using the same logger, or you'll create an infinite loop!
        System.err.println(s"Failed to send log to API: ${e.getMessage}")
    }
  }
}
