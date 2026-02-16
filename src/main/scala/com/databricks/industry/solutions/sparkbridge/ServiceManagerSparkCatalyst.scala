package com.databricks.industry.solutions.fhirapi.sparkbridge

/** SparkBridge module for ServiceManager integration with Spark Catalyst.
  *
  * This module provides utilities for bridging the FHIR service manager with
  * Spark DataFrames and SQL operations.
  *
  * NOTE: This module requires Apache Spark dependencies to be available at
  * runtime.
  *
  * IDE ERRORS: You may see import errors in your IDE for Spark classes. This is
  * expected! This file is part of the 'sparkbridge' subproject which has Spark
  * dependencies in 'provided' scope. The main FHIR REST API project excludes
  * this directory from compilation.
  *
  * To compile this module separately: sbt sparkbridge/compile
  *
  * The IDE has been configured to exclude this package from analysis via:
  *   - .vscode/settings.json (metals.excludedPackages)
  *   - .metals.exclude
  */

import com.databricks.industry.solutions.fhirapi.ServiceManager
import org.apache.spark.sql.catalyst.expressions.{
  Expression,
  LeafExpression,
  UnaryExpression,
  Nondeterministic,
  NullIntolerant
}
import org.apache.spark.sql.catalyst.expressions.codegen.{
  CodegenContext,
  ExprCode
}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

/** Spark Catalyst expression for generating UUIDs in DataFrames.
  *
  * This is a leaf expression (no child expressions) that generates a new UUID
  * for each row by calling ServiceManager.generateUUID.
  *
  * Usage in Spark SQL: SELECT uuid_spark() FROM table
  *
  * Usage in DataFrame: df.withColumn("id", expr("uuid_spark()"))
  *
  * Note: This is marked as Nondeterministic because it generates a new UUID
  * each time it's evaluated, even for the same input.
  */
case class UUIDSpark() extends LeafExpression with Nondeterministic {

  override def nullable: Boolean = false

  override def dataType: DataType = StringType

  override protected def initializeInternal(partitionIndex: Int): Unit = {
    // No initialization needed for UUID generation
  }

  override protected def evalInternal(
      input: org.apache.spark.sql.catalyst.InternalRow
  ): Any = {
    // Call the ServiceManager.generateUUID function and convert to UTF8String
    UTF8String.fromString(ServiceManager.generateUUID)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val serviceManagerClass =
      classOf[ServiceManager.type].getName.stripSuffix("$")
    ev.copy(code = code"""
        |final UTF8String ${ev.value} = UTF8String.fromString(
        |  $serviceManagerClass$$.MODULE$$.generateUUID()
        |);
        |final boolean ${ev.isNull} = false;
      """.stripMargin)
  }

  override def prettyName: String = "uuid_spark"

  override def sql: String = s"$prettyName()"
}

/**
 * Spark Catalyst expression for generating FHIR metadata in DataFrames.
 * 
 * This is a unary expression that takes a UUID string as input and generates
 * FHIR metadata by calling ServiceManager.generateMeta(uuid).
 * 
 * The generated metadata includes:
 * - versionId: The input UUID
 * - lastUpdated: Current timestamp in FHIR format
 * 
 * Usage in Spark SQL:
 *   SELECT meta_spark(versionId) FROM table
 * 
 * Usage in DataFrame:
 *   df.withColumn("meta", expr("meta_spark(version_id)"))
 * 
 * Note: This is marked as Nondeterministic because the lastUpdated timestamp
 * changes each time it's evaluated, even for the same input UUID.
 */
case class MetaSpark(child: Expression) 
    extends UnaryExpression 
    with Nondeterministic 
    with NullIntolerant {
  
  override def nullable: Boolean = false
  
  override def dataType: DataType = StringType
  
  override protected def initializeInternal(partitionIndex: Int): Unit = {
    // No initialization needed
  }
  
  override protected def evalInternal(input: org.apache.spark.sql.catalyst.InternalRow): Any = {
    val uuid = child.eval(input)
    if (uuid == null) {
      null
    } else {
      // Convert the input to a String (UUID), call generateMeta, and convert result to JSON string
      val uuidString = uuid.asInstanceOf[UTF8String].toString
      val metaObj = ServiceManager.generateMeta(uuidString)
      UTF8String.fromString(ujson.write(metaObj))
    }
  }
  
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val serviceManagerClass = classOf[ServiceManager.type].getName.stripSuffix("$")
    
    nullSafeCodeGen(ctx, ev, (uuid) => {
      s"""
        |String uuidStr = $uuid.toString();
        |ujson.Obj metaObj = $serviceManagerClass$$.MODULE$$.generateMeta(uuidStr);
        |${ev.value} = UTF8String.fromString(ujson.package$$.MODULE$$.write(metaObj, 0, false));
      """.stripMargin
    })
  }
  
  override def prettyName: String = "meta_spark"
  
  override def sql: String = s"$prettyName(${child.sql})"
  
  override protected def withNewChildInternal(newChild: Expression): MetaSpark = {
    copy(child = newChild)
  }
}
