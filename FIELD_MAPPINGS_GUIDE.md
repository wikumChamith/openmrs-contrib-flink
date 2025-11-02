# Field Mappings Feature Guide

## Overview

This application now supports **two modes** for defining ETL transformations:

1. **Manual SQL Mode** (existing) - Write custom SQL queries
2. **Field Mappings Mode** (new) - Declarative YAML-based field mappings with automatic SQL generation

Both modes are fully supported and you can choose whichever fits your use case best.

---

## Mode Comparison

| Feature | Manual SQL | Field Mappings |
|---------|-----------|----------------|
| **Flexibility** | Complete control over SQL | Structured, validated mappings |
| **Complexity** | Can be complex for EAV pivots | Simple, declarative YAML |
| **Learning Curve** | Requires SQL + Flink SQL knowledge | Just YAML configuration |
| **Use Case** | Complex transformations, custom logic | Standard EAV pivots, lookups |
| **Maintenance** | Manual updates to SQL | Change mappings in YAML |

---

## Field Mappings Configuration

### YAML Structure

```yaml
fieldMappings:
  # 1. Passthrough fields - direct columns from source table
  passthroughFields:
    - "encounter_id"
    - "patient_id"
    - "encounter_datetime"

  # 2. Concept mappings - EAV pivot from obs table
  conceptMappings:
    - column: "systolic_bp"
      conceptId: 2100
      valueType: "value_numeric"

    - column: "diagnosis_text"
      conceptId: 5089
      valueType: "value_text"

  # 3. Lookup fields - join other tables
  lookupFields:
    - column: "location_name"
      table: "location"
      field: "name"
      joinField: "location_id"

  # 4. Filters - WHERE clause conditions
  filters:
    - "encounter_type = 5"
    - "voided = false"
```

---

## Field Mapping Types

### 1. Passthrough Fields

Direct columns from the source table, selected as-is without transformation.

**Example:**
```yaml
passthroughFields:
  - "encounter_id"
  - "patient_id"
  - "encounter_datetime"
  - "location_id"
```

**Generated SQL:**
```sql
SELECT
  e.encounter_id,
  e.patient_id,
  e.encounter_datetime,
  e.location_id
```

---

### 2. Concept Mappings (EAV Pivot)

Automatically pivots EAV data from the `obs` table into columns.

**Parameters:**
- `column`: Target column name in flattened table
- `conceptId`: OpenMRS concept ID to filter
- `valueType`: Which obs column to use
  - `value_numeric` - for numeric measurements
  - `value_text` - for text observations
  - `value_datetime` - for date/time observations
  - `value_coded` - for coded concept values

**Example:**
```yaml
conceptMappings:
  - column: "systolic_bp"
    conceptId: 2100
    valueType: "value_numeric"

  - column: "diastolic_bp"
    conceptId: 2101
    valueType: "value_numeric"

  - column: "chief_complaint"
    conceptId: 5089
    valueType: "value_text"
```

**Generated SQL:**
```sql
SELECT
  MAX(CASE WHEN o.concept_id = 2100 THEN o.value_numeric END) as systolic_bp,
  MAX(CASE WHEN o.concept_id = 2101 THEN o.value_numeric END) as diastolic_bp,
  MAX(CASE WHEN o.concept_id = 5089 THEN o.value_text END) as chief_complaint
FROM encounter_source e
LEFT JOIN lkp_obs o
  ON e.encounter_id = o.encounter_id
  AND o.voided = false
GROUP BY ...
```

**Requirements:**
- Must include `obs` in `lookupTables` section
- Each concept mapping generates a `CASE WHEN` pivot clause
- Uses `MAX` aggregation (typical for OpenMRS EAV pattern)

---

### 3. Lookup Fields

Join other tables to get related field values using a simple 1:1 mapping.

**Parameters:**
- `column`: Target column name in flattened table
- `table`: Lookup table name (will be prefixed with `lkp_`)
- `field`: Field to select from lookup table
- `joinField`: Field used for joining (must exist in both source and lookup table)

**Example:**
```yaml
lookupFields:
  - column: "location_name"
    table: "location"
    field: "name"
    joinField: "location_id"

  - column: "provider_name"
    table: "provider"
    field: "name"
    joinField: "provider_id"
```

**Generated SQL:**
```sql
SELECT
  location.name as location_name,
  provider.name as provider_name
FROM encounter_source e
LEFT JOIN lkp_location location
  ON e.location_id = location.location_id
LEFT JOIN lkp_provider provider
  ON e.provider_id = provider.provider_id
```

**Requirements:**
- Must include lookup table in `lookupTables` section
- Join pattern assumes: `e.{joinField} = {table}.{joinField}`

---

### 4. Filters

WHERE clause conditions applied to the source table.

**Example:**
```yaml
filters:
  - "encounter_type = 5"
  - "voided = false"
  - "encounter_datetime >= '2024-01-01'"
```

**Generated SQL:**
```sql
WHERE e.encounter_type = 5
  AND e.voided = false
  AND e.encounter_datetime >= '2024-01-01'
```

---

## Complete Examples

### Example 1: Basic Vitals with Field Mappings

**File:** `vitals-job-with-mappings.yaml`

```yaml
connection:
  jdbc: "jdbc:mysql://localhost:3306/openmrs"
  username: "openmrs"
  password: "openmrs"

sourceTable: "encounter"
lookupTables:
  - "obs"

sink:
  jdbc: "jdbc:mysql://localhost:3308/flattened"
  username: "root"
  password: "rootpass"
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

fieldMappings:
  passthroughFields:
    - "encounter_id"
    - "patient_id"
    - "encounter_datetime"

  conceptMappings:
    - column: "systolic_bp"
      conceptId: 2100
      valueType: "value_numeric"

    - column: "diastolic_bp"
      conceptId: 2101
      valueType: "value_numeric"

  filters:
    - "encounter_type = 5"
    - "voided = false"
```

---

### Example 2: With Lookup Tables

**File:** `encounters-with-lookups.yaml`

```yaml
connection:
  jdbc: "jdbc:mysql://localhost:3306/openmrs"
  username: "openmrs"
  password: "openmrs"

sourceTable: "encounter"
lookupTables:
  - "obs"
  - "location"
  - "provider"

sink:
  jdbc: "jdbc:mysql://localhost:3308/flattened"
  username: "root"
  password: "rootpass"
  table: "encounter_vitals_detailed"
  primaryKey:
    - "encounter_id"
  columns:
    - name: "encounter_id"
      type: "INT"
    - name: "systolic_bp"
      type: "DOUBLE"
    - name: "location_name"
      type: "STRING"
    - name: "provider_name"
      type: "STRING"

fieldMappings:
  passthroughFields:
    - "encounter_id"
    - "patient_id"

  conceptMappings:
    - column: "systolic_bp"
      conceptId: 2100
      valueType: "value_numeric"

  lookupFields:
    - column: "location_name"
      table: "location"
      field: "name"
      joinField: "location_id"

    - column: "provider_name"
      table: "provider"
      field: "name"
      joinField: "provider_id"

  filters:
    - "encounter_type = 5"
```

---

## Validation Rules

The system validates your configuration and will reject invalid setups:

### 1. Mutual Exclusivity
- ❌ Cannot have both `sql` and `fieldMappings` in the same YAML
- ✅ Must have exactly one of them

### 2. Field Mappings Requirements
- ✅ Must have at least one of: `passthroughFields`, `conceptMappings`, or `lookupFields`
- ❌ Cannot have empty field mappings

### 3. Concept Mappings Validation
- ✅ `column` cannot be empty
- ✅ `conceptId` cannot be null
- ✅ `valueType` must be one of: `value_numeric`, `value_text`, `value_datetime`, `value_coded`
- ✅ `obs` table must be in `lookupTables`

### 4. Lookup Fields Validation
- ✅ All fields (`column`, `table`, `field`, `joinField`) must be non-empty
- ✅ Referenced table must be in `lookupTables`

### 5. No Duplicate Columns
- ❌ Column names must be unique across all mapping types
- ✅ Each column can only appear once

---

## How It Works

### Architecture

```
YAML File
    ↓
FlinkJobService.parseYaml()
    ↓
[Creates Job entity with FieldMappings]
    ↓
FieldMappingSqlGenerator.validate()
    ↓
FieldMappingSqlGenerator.generateSql()
    ↓
[Auto-generated SQL with pivots, joins, filters]
    ↓
Flink Job Execution
```

### SQL Generation Process

1. **SELECT Clause:**
   - Add passthrough fields: `e.field_name`
   - Add concept pivots: `MAX(CASE WHEN o.concept_id = X THEN o.value_type END)`
   - Add lookup fields: `table.field as column`

2. **FROM Clause:**
   - Source table: `{sourceTable}_source e`

3. **JOIN Clauses:**
   - Obs table (if concepts exist): `LEFT JOIN lkp_obs o ON e.encounter_id = o.encounter_id AND o.voided = false`
   - Lookup tables: `LEFT JOIN lkp_{table} ON e.{joinField} = {table}.{joinField}`

4. **WHERE Clause:**
   - Apply filters with `e.` prefix if not already qualified

5. **GROUP BY Clause:**
   - Include all passthrough fields
   - Include all lookup fields
   - (Concept mappings are aggregated with MAX, so not in GROUP BY)

---

## Migration Guide

### Converting Manual SQL to Field Mappings

**Before (Manual SQL):**
```yaml
sql: |
  SELECT
    e.encounter_id,
    e.patient_id,
    MAX(CASE WHEN o.concept_id = 2100 THEN o.value_numeric END) as systolic_bp
  FROM encounter_source e
  LEFT JOIN lkp_obs o ON e.encounter_id = o.encounter_id
  WHERE e.encounter_type = 5
  GROUP BY e.encounter_id, e.patient_id
```

**After (Field Mappings):**
```yaml
fieldMappings:
  passthroughFields:
    - "encounter_id"
    - "patient_id"

  conceptMappings:
    - column: "systolic_bp"
      conceptId: 2100
      valueType: "value_numeric"

  filters:
    - "encounter_type = 5"
```

---

## When to Use Each Mode

### Use **Manual SQL** when:
- ✅ You need complex calculations or expressions
- ✅ You need custom aggregations (SUM, COUNT with conditions)
- ✅ You need UNION, subqueries, or window functions
- ✅ You need fine-grained control over join conditions
- ✅ You have non-standard data patterns

### Use **Field Mappings** when:
- ✅ Standard OpenMRS EAV pivot pattern
- ✅ Simple 1:1 lookup table joins
- ✅ Straightforward passthrough fields
- ✅ You want validated, maintainable configuration
- ✅ You prefer declarative over imperative style

---

## Database Schema

Field mappings are persisted in these tables:

- `field_mapping_passthrough` - Stores passthrough field names
- `field_mapping_concept` - Stores concept mappings (column, conceptId, valueType)
- `field_mapping_lookup` - Stores lookup field mappings
- `field_mapping_filter` - Stores WHERE clause filters

All tables use `job_id` as the foreign key to link to the parent job.

---

## Troubleshooting

### Error: "obs is not in lookupTables"
**Solution:** Add `obs` to the `lookupTables` section:
```yaml
lookupTables:
  - "obs"
```

### Error: "Configuration cannot contain both 'sql' and 'fieldMappings'"
**Solution:** Remove either `sql:` or `fieldMappings:` section - you can only use one.

### Error: "Duplicate column name"
**Solution:** Ensure each column name is unique across all mapping types.

### Error: "Invalid valueType"
**Solution:** Use one of: `value_numeric`, `value_text`, `value_datetime`, `value_coded`

### Error: "Field mappings reference lookup table 'X' but it's not in lookupTables"
**Solution:** Add the table to `lookupTables`:
```yaml
lookupTables:
  - "obs"
  - "X"  # Add your table here
```

---

## API Usage

Upload a YAML file with field mappings:

```bash
curl -X POST http://localhost:8081/api/jobs/upload \
  -F "file=@vitals-job-with-mappings.yaml"
```

Response:
```json
{
  "success": true,
  "message": "Job registered successfully",
  "jobId": 1,
  "sourceTable": "encounter",
  "sinkTable": "vitals"
}
```

The generated SQL will be logged in the application logs with `DEBUG` level.

---

## Future Extensibility (Option 1 Upgrade Path)

The current implementation (Option 2) can be extended in the future to support:

- **Aggregate Mappings** - COUNT, SUM, AVG over related records
- **Custom Join Conditions** - Complex join logic
- **Calculated Fields** - Expressions like CONCAT, math operations
- **Advanced Lookups** - Multi-column joins, outer applies

All existing field mappings will continue to work when these features are added.

---

## Summary

Field Mappings provide a **simpler, validated way** to define common ETL patterns in OpenMRS:
- No need to write complex CASE WHEN SQL
- Automatic validation of configuration
- Clean, maintainable YAML structure
- Full backward compatibility with manual SQL

Both modes will always be supported - choose what works best for your use case!
