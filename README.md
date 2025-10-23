<img src=https://raw.githubusercontent.com/databricks-industry-solutions/.github/main/profile/solacc_logo.png width="600px">

[![DBR](https://img.shields.io/badge/DBR-CHANGE_ME-red?logo=databricks&style=for-the-badge)](https://docs.databricks.com/release-notes/runtime/CHANGE_ME.html)
[![CLOUD](https://img.shields.io/badge/CLOUD-CHANGE_ME-blue?logo=googlecloud&style=for-the-badge)](https://databricks.com/try-databricks)

## Scope

This service implements a FHIR-compliant RESTful API on Databricks, adhering to the [FHIR RESTful API specification](https://hl7.org/fhir/http.html). The implementation supports the following FHIR interaction types:

### Instance Level Interactions
Operations on individual resource instances identified by their logical ID:
- [x] **read**: Read the current state of a specific resource (e.g., `GET /fhir/Patient/[id]`)
- [ ] **vread**: Read a specific version of a resource
- [ ] **update**: Update an existing resource by its ID (or create if new)
- [ ] **patch**: Update a resource by posting changes
- [ ] **delete**: Delete a specific resource
- [ ] **history**: Retrieve change history for a resource

### Type Level Interactions
Operations across all instances of a specific resource type:
- [ ] **create**: Create a new resource with server-assigned ID
- [ ] **search**: Search resources of a type based on filter criteria
- [ ] **history**: Retrieve change history for a resource type

### Whole System Interactions
Operations that span across the entire FHIR server:
- [ ] **capabilities**: Get capability statement for the system (metadata endpoint)
- [ ] **batch/transaction**: Perform multiple operations in a single request
- [ ] **search**: Search across all resource types
- [ ] **history**: Retrieve change history for all resources

### Extended Operations
- [ ] **$everything**: Patient compartment operation to retrieve all resources related to a patient

For complete FHIR specification details, see: https://hl7.org/fhir/http.html

## Architecture

Understanding how the components work together:
- **[Simplified Architecture Diagrams](img/architecture-simplified.md)** - Visual flow diagrams showing request lifecycle
- **[ASCII Architecture Flow](img/architecture-flow.txt)** - Text-based diagrams for quick reference

### Quick Navigation
| Want to understand... | Start with this file |
|----------------------|---------------------|
| 📍 Overall architecture | `ServiceManager.scala` |
| 🔍 URL to SQL translation | `QueryInterpreter.scala` |
| ⚙️ Query execution | `QueryRunner.scala` |
| 🗄️ Database connections | `DataStore.scala` |
| 📋 FHIR formatting | `FormatManager.scala` |
| 🌐 API routes | `FhirRestApi.scala` |

### Configuring

The service uses environment variables to configure connections to Databricks and API behavior. All settings have defaults in [`application.conf`](src/main/resources/application.conf) that can be overridden via environment variables set prior to launching the application.

#### Required: Databricks Connection

To connect to your Databricks SQL Warehouse:

| Environment Variable | Description | Example |
|---------------------|-------------|---------|
| `jdbc` | JDBC connection string for your Databricks SQL Warehouse | `jdbc:databricks://<workspace>.cloud.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/<warehouse-id>` |
| `token` | Databricks personal access token or service principal token | `dapi1234567890abcdef...` |

**How to get these values:**
1. In Databricks workspace, navigate to **SQL Warehouses**
2. Select your warehouse and click **Connection Details**
3. Copy the JDBC URL and set as `jdbc` environment variable
4. Generate a Personal Access Token from **User Settings → Access Tokens**

#### Required: Data Location

Specify where FHIR resource tables are stored:

| Environment Variable | Description | Default |
|---------------------|-------------|---------|
| `catalog` | Unity Catalog name | `hls_healthcare` |
| `schema` | Schema containing FHIR resource tables | `databricks_fhir_service_forked` |

These should match the location where you loaded your FHIR data using `create_fhir_data_backend.py`.

#### Optional: HTTP Server Configuration

| Environment Variable | Description | Default |
|---------------------|-------------|---------|
| `interface` | Network interface to bind to | `0.0.0.0` |
| `port` | Port number for the API server | `9000` |
| `request-timeout` | Maximum request timeout | `60s` |

#### Optional: Connection Pool Settings

Tune Hikari connection pool for your workload:

| Environment Variable | Description | Default |
|---------------------|-------------|---------|
| `minIdle` | Minimum idle connections | `1` |
| `maxPoolSize` | Maximum connection pool size | `-1` (2x CPUs) |
| `timeoutMS` | Connection timeout in milliseconds | `30000` |
| `connectionRetries` | Number of connection retry attempts | `1` |
| `queryRetries` | Number of query retry attempts | `1` |

#### Optional: Logging

| Environment Variable | Description | Default |
|---------------------|-------------|---------|
| `loglevel` | Akka logging level (DEBUG, INFO, WARNING, ERROR) | `DEBUG` |

#### Example Configuration

Create a `.env` file or export these variables before starting the service:

```bash
# Databricks Connection (REQUIRED)
export jdbc="jdbc:databricks://your-workspace.cloud.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/abc123def456"
export token="dapi..."

# Data Location (REQUIRED)
export catalog="hls_healthcare"
export schema="databricks_fhir_service"

# HTTP Server (OPTIONAL)
export port="9000"
export interface="0.0.0.0"

# Connection Pool (OPTIONAL)
export maxPoolSize="10"
export timeoutMS="60000"

# Logging (OPTIONAL)
export loglevel="INFO"
```

### Deploying

(1) Is app running? 
```python
#http://localhost:9000/debug/test -> 
{"status": "FHIR API is running!"}
```

(2) Can the app get a conncetion to a DBSQL Warehouse?
```python
#http://localhost:9000/debug/dbsqlConnect -> 
com.databricks.client.hivecommon.jdbc42.Hive42Connection@1f0f417e
```

(3) Debug info for a given request
```python
#http://localhost:9000/debug/patient/a62a41dc-5ac1-ff47-3fc5-08f6ad045571 -> 
queryRuntime (in ms): 2444
queryStartTime: 2025-04-17T10:05:38.241-04:00
queryError: None
numRows: 1
queryExecuted: SELECT to_json(patient) AS resultset FROM hls_healthcare.databricks_fhir_service_forked.patient WHERE id = 'a62a41dc-5ac1-ff47-3fc5-08f6ad045571'
```

(4) Running a read request
```python
#http://localhost:9000/fhir/patient/a62a41dc-5ac1-ff47-3fc5-08f6ad045571 -> 
{"id":"a62a41dc-5ac1-ff47-3fc5-08f6ad045571","meta":{"profile":["http://hl7.org/fhir...
```

(5) Running a $everthing request 
```python
#http://localhost:9000/fhir/patient/a62a41dc-5ac1-ff47-3fc5-08f6ad045571/$everything
{"resourceType":"Bundle","type":"searchset","entry":[{"resource":{"id"....

```

## Loading Backend Data

Backend data is loaded through Databricks compute using the [`create_fhir_data_backend.py`](scripts/create_fhir_data_backend.py) script, which leverages the [`dbignite`](https://github.com/databrickslabs/dbignite) Python package to ingest FHIR-compliant resources.

### How It Works

The script performs the following operations:

1. **Reads FHIR Resources**: Uses `dbignite.readers.read_from_directory()` to read FHIR-compliant JSON files from a source location (e.g., S3, DBFS)
2. **Parses Bundle Entries**: Applies the `FhirSchemaModel` (R4 schema) to parse and structure the FHIR bundle entries
3. **Writes to Databricks Tables**: Creates one table per FHIR resource type (e.g., `Patient`, `Observation`, `Encounter`) in the target schema
4. **Parallel Processing**: Uses multithreading to write multiple resource types concurrently for improved performance

### Usage

The script appends FHIR resources into Databricks tables with the following structure:
```
<catalog>.<schema>.<resource_type>
```

For example:
- `hls_healthcare.databricks_fhir_service.patient`
- `hls_healthcare.databricks_fhir_service.observation`
- `hls_healthcare.databricks_fhir_service.encounter`

Each table contains the parsed FHIR resource data with an `id` column that serves as the resource identifier for API queries.

**Note**: This is a backend data loading process, not a REST API function. Data loading is performed separately from API read operations.


## Project support 

Please note the code in this project is provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs). They are provided AS-IS and we do not make any guarantees of any kind. Please do not submit a support ticket relating to any issues arising from the use of these projects. The source in this project is provided subject to the Databricks [License](./LICENSE.md). All included or referenced third party libraries are subject to the licenses set forth below.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo. They will be reviewed as time permits, but there are no formal SLAs for support. 

## License

&copy; 2024 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.

| library                                | description             | license    | source                                              |
|----------------------------------------|-------------------------|------------|-----------------------------------------------------|
