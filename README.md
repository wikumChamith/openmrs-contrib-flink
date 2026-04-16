# OpenMRS Flink ETL

A real-time ETL tool that flattens OpenMRS EAV (Entity-Attribute-Value) data into analytics-ready tables using Apache Flink CDC.

## What It Does

- Streams data changes from OpenMRS MySQL database in real-time
- Transforms EAV observations into flat columns
- Writes to a target database with upsert support
- No coding required - just write YAML job configs

## Quick Start

### Prerequisites

- Docker and Docker Compose
- OpenMRS instance with MySQL/MariaDB (binlog enabled)

### Run with Docker

```bash
docker-compose up -d
```

This starts:
- OpenMRS stack (gateway, frontend, backend, db)
- Flink application (port 8081)
- Target database for flattened data

### Access the UI

Open [http://localhost:8081](http://localhost:8081) in your browser.

From the UI you can:
- Upload and submit job YAML files
- View running jobs and their status
- Stop or delete jobs
- Manage secrets for secure credential storage

---

## Writing ETL Jobs

Jobs are defined in YAML. Two approaches available:

| Approach | Best For |
|----------|----------|
| Field Mappings | Simple EAV pivots, lookups |
| Manual SQL | Complex joins, aggregations |

### Job Structure

Every job needs these sections:

```yaml
connection:       # Source database connection
sourceTable:      # Main CDC table to stream
lookupTables:     # Tables to join (optional)
sink:             # Target table definition
fieldMappings:    # OR sql: - transformation logic
```

### Secrets

Credentials can be stored as named secrets and referenced in YAML configs instead of hardcoding plaintext passwords. Secrets are encrypted at rest (AES-256-GCM) and never exposed via the API.

**1. Create a secret** (via API or UI):

```bash
curl -X POST http://localhost:8081/api/secrets \
  -H "Content-Type: application/json" \
  -d '{"name": "OPENMRS_DB_PASSWORD", "value": "openmrs"}'
```

**2. Reference it in your YAML:**

```yaml
connection:
  jdbc: "jdbc:mysql://db:3306/openmrs"
  username: "${{ secrets.OPENMRS_DB_USERNAME }}"
  password: "${{ secrets.OPENMRS_DB_PASSWORD }}"
```

Jobs uploaded with plaintext credentials will display a warning suggesting the use of secret references. See [`vitals-with-secrets.yaml`](src/main/resources/sample/vitals-with-secrets.yaml) for a full example.

---

## Example 1: Field Mappings Approach

Best for straightforward EAV pivot with concept mappings.

```yaml
connection:
  jdbc: "jdbc:mysql://db:3306/openmrs"
  username: "openmrs"
  password: "openmrs"

sourceTable: "encounter"

lookupTables:
  - "obs"
  - "location"

sink:
  jdbc: "jdbc:mysql://db:3306/flattened"
  username: "openmrs"
  password: "openmrs"
  table: "vitals"
  primaryKey:
    - "encounter_id"
  columns:
    - name: "encounter_id"
      type: "INT"
    - name: "patient_id"
      type: "INT"
    - name: "systolic_bp"
      type: "DOUBLE"
    - name: "diastolic_bp"
      type: "DOUBLE"
    - name: "location_name"
      type: "STRING"

fieldMappings:
  # Direct columns from source table
  passthroughFields:
    - "encounter_id"
    - "patient_id"

  # Pivot EAV obs rows into columns
  conceptMappings:
    - column: "systolic_bp"
      conceptId: 5085
      valueType: "value_numeric"

    - column: "diastolic_bp"
      conceptId: 5086
      valueType: "value_numeric"

  # Join with lookup tables
  lookupFields:
    - column: "location_name"
      table: "location"
      field: "name"
      joinField: "location_id"

  # Filter conditions
  filters:
    - "encounter_type = 5"
    - "voided = false"
```

### Field Mappings Options

| Field | Description |
|-------|-------------|
| `passthroughFields` | Columns copied directly from source |
| `conceptMappings` | EAV pivot - map concept IDs to columns |
| `lookupFields` | Join and pull fields from lookup tables |
| `filters` | WHERE conditions |

### Concept Mapping

```yaml
conceptMappings:
  - column: "weight_kg"        # Output column name
    conceptId: 5089            # OpenMRS concept ID
    valueType: "value_numeric" # obs value column
```

Use `conceptUuid` instead of `conceptId` for portability:

```yaml
  - column: "weight_kg"
    conceptUuid: "5089AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
    valueType: "value_numeric"
```

### Lookup Fields

Join with other tables:

```yaml
lookupFields:
  - column: "location_name"    # Output column
    table: "location"          # Lookup table
    field: "name"              # Field to fetch
    joinField: "location_id"   # Join key
```

---

## Example 2: Manual SQL Approach

Best for complex transformations, aggregations, or multi-table joins.

```yaml
connection:
  jdbc: "jdbc:mysql://db:3306/openmrs"
  username: "openmrs"
  password: "openmrs"

sourceTable: "encounter"

lookupTables:
  - "obs"

sink:
  jdbc: "jdbc:mysql://db:3306/flattened"
  username: "openmrs"
  password: "openmrs"
  table: "vitals"
  primaryKey:
    - "encounter_id"
  columns:
    - name: "encounter_id"
      type: "INT"
    - name: "patient_id"
      type: "INT"
    - name: "systolic_bp"
      type: "DOUBLE"
    - name: "diastolic_bp"
      type: "DOUBLE"

sql: |
  SELECT
    e.encounter_id,
    e.patient_id,
    MAX(CASE WHEN o.concept_id = 5085 THEN o.value_numeric END) as systolic_bp,
    MAX(CASE WHEN o.concept_id = 5086 THEN o.value_numeric END) as diastolic_bp
  FROM encounter_source e
  LEFT JOIN lkp_obs o
    ON e.encounter_id = o.encounter_id
    AND o.voided = false
  WHERE e.encounter_type = 5
    AND e.voided = false
  GROUP BY
    e.encounter_id,
    e.patient_id
```

### SQL Table Naming

| Table Type | Naming Convention |
|------------|-------------------|
| Source table | `{table}_source` (e.g., `encounter_source`) |
| Lookup tables | `lkp_{table}` (e.g., `lkp_obs`) |

---

## Column Types

Supported types for sink columns:

| Type | Description |
|------|-------------|
| `INT` | Integer |
| `BIGINT` | Large integer |
| `DOUBLE` | Decimal number |
| `STRING` | Text |
| `BOOLEAN` | True/false |
| `DATE` | Date only |
| `TIMESTAMP(3)` | Date and time |

---

## Sample Jobs

See [`src/main/resources/sample/`](src/main/resources/sample/) for examples:

| File | Description |
|------|-------------|
| [vitals-by-concept-id.yaml](src/main/resources/sample/vitals-by-concept-id.yaml) | Vitals using concept ID mappings |
| [vitals-by-concept-uuid.yaml](src/main/resources/sample/vitals-by-concept-uuid.yaml) | Vitals using concept UUIDs |
| [vitals-manual-sql.yaml](src/main/resources/sample/vitals-manual-sql.yaml) | Vitals using manual SQL |
| [patient-demographics.yaml](src/main/resources/sample/patient-demographics.yaml) | Patient with name and address |
| [encounter-vitals-with-location.yaml](src/main/resources/sample/encounter-vitals-with-location.yaml) | Encounters with location lookup |
| [vitals-with-secrets.yaml](src/main/resources/sample/vitals-with-secrets.yaml) | Vitals using secret references |

---

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/jobs/upload` | Upload YAML job config (multipart file) |
| GET | `/api/jobs` | List all jobs (passwords masked) |
| DELETE | `/api/jobs/{id}` | Stop and remove job |
| POST | `/api/secrets` | Create or update a secret |
| GET | `/api/secrets` | List secret names (values never exposed) |
| DELETE | `/api/secrets/{name}` | Delete a secret |