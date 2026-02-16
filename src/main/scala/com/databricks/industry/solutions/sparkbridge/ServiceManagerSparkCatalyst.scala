package com.databricks.industry.solutions.fhirapi.sparkbridge

/**
 * SparkBridge module for ServiceManager integration with Spark Catalyst.
 * 
 * This module provides utilities for bridging the FHIR service manager
 * with Spark DataFrames and SQL operations.
 * 
 * NOTE: This module requires Apache Spark dependencies to be available at runtime.
 * The imports are commented out to allow compilation without Spark on the classpath.
 * Uncomment when Spark dependencies are added to build.sbt.
 */

// Uncomment when Spark dependencies are available:
import com.databricks.industry.solutions.fhirapi.ServiceManager
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression, ExpectsInputTypes}
import org.apache.spark.sql.types.{DataType, AbstractDataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

case class Foo(bar: String)
