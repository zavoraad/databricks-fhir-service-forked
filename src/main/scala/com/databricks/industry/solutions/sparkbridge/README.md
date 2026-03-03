# SparkBridge Module

This directory contains Spark-specific code for integrating the FHIR service with Apache Spark DataFrames and Catalyst expressions.

## Purpose

The SparkBridge module provides utilities for:
- UUID generation in Spark UDFs
- Metadata generation for FHIR resources in Spark
- Batch operations using Spark DataFrames
- Integration with Databricks Catalyst optimizer

## Build Configuration

This module is configured as a **separate subproject** in `build.sbt`:

- **Dependencies**: Apache Spark (spark-sql, spark-catalyst) with `provided` scope
- **Compilation**: Excluded from the main FHIR REST API build
- **Runtime**: Spark dependencies are available at runtime in Databricks environments

## IDE Configuration

⚠️ **You may see import errors in your IDE for Spark classes. This is expected!**

The main project excludes this directory from compilation, so IDEs like Cursor/VSCode with Metals will show errors. The module compiles correctly when built separately.

### Configuration Files
- `.vscode/settings.json` - Excludes this package from Metals analysis
- `.metals.exclude` - Tells Metals to skip this package

## Usage

### Compile the SparkBridge module
```bash
sbt sparkbridge/compile
```

### Build only the main FHIR REST API (without Spark)
```bash
sbt compile
sbt assembly
```

### Use in Databricks
The compiled code can be deployed to Databricks where Spark dependencies are available at runtime.

## Architecture

```
root project (databricks-fhir-service)
├── Main FHIR REST API
├── Akka HTTP, JDBC, etc.
└── NO Spark dependencies

sparkbridge subproject
├── Spark SQL integration
├── Catalyst expressions
└── Depends on root + Spark (provided)
```

This separation ensures the REST API Docker image remains lightweight without Spark libraries (~300MB+).
