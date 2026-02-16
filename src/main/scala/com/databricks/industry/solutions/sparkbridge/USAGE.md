## Spark Catalyst Expressions for FHIR

This module provides Spark Catalyst expressions that bridge to `ServiceManager` functions, allowing FHIR operations to be used in Spark DataFrames and SQL queries.

---

## UUIDSpark - UUID Generation

The `UUIDSpark` case class is a Spark Catalyst expression that bridges to the `ServiceManager.generateUUID` function, allowing UUID generation to be used in Spark DataFrames and SQL queries.

### Architecture

```
Spark DataFrame/SQL Query
        ↓
   UUIDSpark (Catalyst Expression)
        ↓
   ServiceManager.generateUUID
        ↓
   java.util.UUID.randomUUID()
```

### Usage Examples

#### 1. Register Functions in Databricks

```scala
import com.databricks.industry.solutions.fhirapi.sparkbridge.{UUIDSpark, MetaSpark}
import org.apache.spark.sql.catalyst.expressions.Expression

// Register UUID generator
spark.sessionState.functionRegistry.registerFunction(
  "uuid_spark",
  (expressions: Seq[Expression]) => UUIDSpark()
)

// Register metadata generator
spark.sessionState.functionRegistry.registerFunction(
  "meta_spark",
  (expressions: Seq[Expression]) => MetaSpark(expressions.head)
)
```

#### 2. Use in Spark SQL

```sql
-- Generate UUIDs for each row in a table
SELECT 
  uuid_spark() as id,
  resourceType,
  data
FROM fhir_resources;

-- Create a new FHIR resource with auto-generated ID and metadata
CREATE TABLE new_patients AS
SELECT
  uuid_spark() as id,
  'Patient' as resourceType,
  meta_spark(uuid_spark()) as meta,
  to_json(struct(*)) as resource
FROM patient_data;
```

#### 3. Use in DataFrame Operations

```scala
import org.apache.spark.sql.functions.expr

// Add UUID column to existing DataFrame
val df = spark.table("fhir_resources")
val dfWithUUID = df.withColumn("id", expr("uuid_spark()"))

// Create new records with UUIDs
val newRecords = spark.sql("""
  SELECT 
    uuid_spark() as id,
    'Observation' as resourceType,
    value,
    timestamp
  FROM observations
""")
```

#### 4. Use in Batch Inserts

```scala
// Generate UUIDs for batch insert of FHIR resources
val batchData = spark.read.json("s3://bucket/incoming/*.json")

val withIds = batchData
  .withColumn("id", expr("uuid_spark()"))
  .withColumn("resourceType", lit("Claim"))

withIds.write
  .format("delta")
  .mode("append")
  .saveAsTable("catalog.schema.Claim")
```

### Features

- **Nondeterministic**: Generates a new UUID for each row evaluation
- **Null-safe**: Never returns null
- **Code Generation**: Optimized with Catalyst code generation for performance
- **Type-safe**: Returns `StringType` (UTF8String internally)

---

## MetaSpark - FHIR Metadata Generation

The `MetaSpark` case class is a Spark Catalyst expression that bridges to the `ServiceManager.generateMeta` function, generating FHIR metadata with versionId and lastUpdated timestamp.

### Architecture

```
Spark DataFrame/SQL Query (with UUID input)
        ↓
   MetaSpark (Catalyst Expression)
        ↓
   ServiceManager.generateMeta(uuid)
        ↓
   JSON: {"versionId": "...", "lastUpdated": "2026-02-16T15:19:00.123-08:00"}
```

### Usage Examples

#### 1. Generate Metadata with Version ID

```sql
-- Add metadata to existing resources
SELECT 
  id,
  resourceType,
  meta_spark(version_id) as meta,
  resource
FROM fhir_resources;

-- Generate complete FHIR resource with metadata
SELECT
  uuid_spark() as id,
  meta_spark(uuid_spark()) as meta,
  'Patient' as resourceType,
  name,
  birthDate
FROM patient_source;
```

#### 2. Use in DataFrame Operations

```scala
import org.apache.spark.sql.functions.{expr, col}

val df = spark.table("fhir_resources")

// Generate version ID and corresponding metadata
val dfWithMeta = df
  .withColumn("versionId", expr("uuid_spark()"))
  .withColumn("meta", expr("meta_spark(versionId)"))

// Or use a single UUID for both
val dfWithSingleUUID = df
  .withColumn("versionUUID", expr("uuid_spark()"))
  .withColumn("meta", expr("meta_spark(versionUUID)"))
```

#### 3. Batch Update with New Metadata

```scala
// Update metadata for all resources in a table
spark.sql("""
  UPDATE catalog.schema.Patient
  SET 
    meta = meta_spark(uuid_spark()),
    lastModified = current_timestamp()
  WHERE active = true
""")
```

#### 4. Complete FHIR Resource Creation

```scala
val newPatients = spark.sql("""
  SELECT
    uuid_spark() as id,
    meta_spark(uuid_spark()) as meta,
    'Patient' as resourceType,
    struct(
      array(struct(
        family,
        array(given) as given
      )) as name
    ) as resource
  FROM source_patient_data
""")

newPatients.write
  .format("delta")
  .mode("append")
  .saveAsTable("catalog.schema.Patient")
```

### Features

- **Unary Expression**: Takes a UUID string as input
- **Nondeterministic**: Generates new timestamp on each evaluation
- **Null-safe**: Returns null if input is null (NullIntolerant)
- **FHIR-Compliant**: Generates metadata in FHIR format with ISO 8601 timestamps
- **Code Generation**: Optimized with Catalyst code generation

### Output Format

The `meta_spark` function generates JSON with this structure:

```json
{
  "versionId": "550e8400-e29b-41d4-a716-446655440000",
  "lastUpdated": "2026-02-16T15:19:00.123-08:00"
}
```

---

## Performance Considerations

- Both expressions use Spark's code generation (`doGenCode`) for optimal performance
- UUIDs and timestamps are generated independently per row
- For very large datasets (billions of rows), consider pre-generating values in batches if you need deterministic replay
- The expressions are marked as `Nondeterministic`, which affects Spark's optimization strategies

## Integration with FHIR API

These Catalyst expressions use the same logic as the REST API's `ServiceManager`, ensuring consistency across:
- REST API resource creation
- Batch Spark operations  
- Databricks SQL queries

All systems use:
- `java.util.UUID.randomUUID()` (version 4 UUID) for IDs
- `ZonedDateTime.now()` with ISO 8601 formatting for timestamps
