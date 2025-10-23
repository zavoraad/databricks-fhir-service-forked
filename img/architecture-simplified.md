# FHIR API - Simplified Architecture

## Request Flow Diagram

```mermaid
flowchart TD
    A[HTTP Request<br/>GET /fhir/Patient/123] --> B[FhirRestApi<br/>Entry Point<br/>Routes & Config]
    B --> C[ServiceManager<br/>Orchestrator]
    
    C --> D[QueryInterpreter<br/>Build SQL]
    C --> E[QueryRunner<br/>Execute Query]
    C --> F[FormatManager<br/>Format Output]
    
    D --> G{SQL Query<br/>SELECT * FROM patient<br/>WHERE id = '123'}
    G --> E
    
    E --> H[DataStore<br/>Connection Pool<br/>& Retry Logic]
    H --> I[Auth<br/>TokenAuth<br/>JDBC Connection]
    I --> J[(Databricks<br/>SQL Warehouse)]
    
    J --> I
    I --> H
    H --> E
    
    E --> K[QueryOutput<br/>Results + Stats]
    K --> F
    
    F --> L[FHIR Response<br/>JSON/Bundle/NDJSON]
    L --> M[HTTP Response<br/>200 OK]
    
    style A fill:#e1f5ff
    style B fill:#ffecb3
    style C fill:#ffecb3
    style D fill:#c8e6c9
    style E fill:#f8bbd0
    style F fill:#d1c4e9
    style J fill:#f8bbd0
    style M fill:#e1f5ff
```

## Component Overview

```mermaid
graph LR
    subgraph "Entry Layer"
        A[FhirRestApi]
    end
    
    subgraph "Orchestration Layer"
        B[ServiceManager]
    end
    
    subgraph "Processing Layer"
        C[QueryInterpreter<br/>SQL Builder]
        D[QueryRunner<br/>Executor]
        E[FormatManager<br/>Formatter]
    end
    
    subgraph "Data Layer"
        F[DataStore]
        G[Auth]
        H[(Database)]
    end
    
    A --> B
    B --> C
    B --> D
    B --> E
    D --> F
    F --> G
    G --> H
    
    style A fill:#ffecb3
    style B fill:#ffecb3
    style C fill:#c8e6c9
    style D fill:#f8bbd0
    style E fill:#d1c4e9
    style F fill:#f8bbd0
    style G fill:#f8bbd0
    style H fill:#ffcdd2
```

## Simplified Component Interaction

```mermaid
sequenceDiagram
    participant Client
    participant API as FhirRestApi
    participant SM as ServiceManager
    participant QI as QueryInterpreter
    participant QR as QueryRunner
    participant DS as DataStore
    participant DB as Database
    participant FM as FormatManager
    
    Client->>API: GET /fhir/Patient/123
    API->>SM: Route request
    
    par Build & Execute & Format
        SM->>QI: Parse URL
        QI-->>SM: SQL Query
        SM->>QR: Execute query
        QR->>DS: Run SQL
        DS->>DB: Execute
        DB-->>DS: Results
        DS-->>QR: QueryOutput
        QR-->>SM: Results
        SM->>FM: Format results
        FM-->>SM: FHIR JSON
    end
    
    SM-->>API: Response
    API-->>Client: 200 OK + FHIR Resource
```

## Key Components (Simple View)

| Component | What It Does | Example |
|-----------|--------------|---------|
| **FhirRestApi** | Receives HTTP requests | `GET /fhir/Patient/123` → route to handler |
| **ServiceManager** | Coordinates everything | Calls Interpreter → Runner → Formatter |
| **QueryInterpreter** | URL → SQL | `/Patient/123` → `SELECT ... WHERE id='123'` |
| **QueryRunner** | Executes SQL | Runs query, returns results + stats |
| **DataStore** | Database connection | Connection pool + retry logic |
| **Auth** | Authentication | Token-based JDBC auth |
| **FormatManager** | SQL results → FHIR | JSON results → FHIR Bundle/Resource |

## Navigation Guide

### Where do I look for...?

| Question | File to Check |
|----------|---------------|
| "How do URLs become SQL queries?" | `QueryInterpreter.scala` |
| "How do queries run?" | `QueryRunner.scala` |
| "How do we connect to the database?" | `DataStore.scala` + `Auth.scala` |
| "How do results become FHIR format?" | `FormatManager.scala` |
| "What are all the API endpoints?" | `FhirRestApi.scala` |
| "How does it all fit together?" | `ServiceManager.scala` (START HERE!) |

## Request Example: Step by Step

### 1. Client Request
```http
GET /fhir/Patient/a62a41dc-5ac1-ff47-3fc5-08f6ad045571
```

### 2. FhirRestApi
- Routes to Patient handler
- Loads config

### 3. ServiceManager
- Calls `QueryInterpreter.read("Patient", "a62a41dc-5ac1...")`
- Gets SQL query back
- Passes to `QueryRunner`

### 4. QueryInterpreter
```scala
// Builds:
"SELECT to_json(patient) AS resultset 
 FROM hls_healthcare.databricks_fhir_service_forked.patient 
 WHERE id = 'a62a41dc-5ac1-ff47-3fc5-08f6ad045571'"
```

### 5. QueryRunner → DataStore
- Executes SQL via connection pool
- Returns `QueryOutput` with results + metadata

### 6. FormatManager
- Takes SQL results
- Formats as FHIR Resource (JSON)
- Adds proper status codes

### 7. Response
```json
{
  "id": "a62a41dc-5ac1-ff47-3fc5-08f6ad045571",
  "meta": {"profile": ["http://hl7.org/fhir..."]},
  "resourceType": "Patient",
  ...
}
```

---

## Architecture Principles

1. **Separation of Concerns**: Each component has one job
2. **ServiceManager is the Hub**: Everything flows through it
3. **Three-Stage Pipeline**: Parse → Execute → Format
4. **Database Abstraction**: DataStore/Auth hide DB complexity
5. **FHIR Compliance**: FormatManager ensures spec compliance

