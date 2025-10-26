# Docker Setup Guide

This guide explains how to run the complete OpenMRS CDC ETL system using Docker.

## Architecture

```
┌─────────────────┐      ┌──────────────────┐      ┌─────────────────┐
│  OpenMRS O3     │      │  Your Flink CDC  │      │   Flattened DB  │
│  (Source)       │─────▶│  Application     │─────▶│   (Target)      │
│  Port: 8080     │ CDC  │  Port: 8081      │ JDBC │   Port: 3308    │
└─────────────────┘      └──────────────────┘      └─────────────────┘
        │                         │
        │                         │
        ▼                         ▼
┌─────────────────┐      ┌─────────────────┐
│  OpenMRS DB     │      │   App Metadata  │
│  (Source Data)  │      │   DB (Jobs)     │
│  Port: 3306     │      │   Port: 3307    │
└─────────────────┘      └─────────────────┘
```

## Services

### 1. **openmrs-db** (Port 3306)
- **Database**: MariaDB 10.11
- **Purpose**: OpenMRS source database with CDC enabled (binlog)
- **Credentials**:
  - User: `openmrs` / Password: `Admin123`
  - Root: `root` / Password: `Admin123`
- **Features**: Binary logging enabled for CDC

### 2. **openmrs** (Port 8080)
- **Application**: OpenMRS Reference Application 3.0.0
- **URL**: http://localhost:8080/openmrs
- **Default Login**:
  - Username: `admin`
  - Password: `Admin123`
- **Purpose**: Create/edit clinical data (encounters, observations, etc.)

### 3. **target-db** (Port 3308)
- **Database**: MariaDB 10.11
- **Purpose**: Destination for flattened/transformed data
- **Credentials**:
  - User: `flink_user` / Password: `flink_pass`
  - Root: `root` / Password: `rootpass`
- **Database**: `flattened`

### 4. **app-db** (Port 3307)
- **Database**: MySQL 8.0
- **Purpose**: Store Flink job metadata
- **Credentials**:
  - User: `flink_user` / Password: `flink_pass`
  - Root: `root` / Password: `rootpass`
- **Database**: `openmrs_flink`

## Getting Started

### Step 1: Start All Services

```bash
docker-compose up -d
```

**Wait time**: OpenMRS initialization takes 3-5 minutes on first start.

Check status:
```bash
docker-compose ps
```

Check OpenMRS logs:
```bash
docker-compose logs -f openmrs
```

Wait for: `INFO: Server startup in [XXXXX] milliseconds`

### Step 2: Access OpenMRS O3

1. Open browser: http://localhost:8080/openmrs
2. Login:
   - Username: `admin`
   - Password: `Admin123`
3. Create test data:
   - Register patients
   - Record vitals encounters (encounter_type = 5)
   - Add observations (blood pressure, temperature, etc.)

### Step 3: Configure Your Application

Update `application.properties`:

```properties
# Application metadata database
spring.datasource.url=jdbc:mysql://localhost:3307/openmrs_flink
spring.datasource.username=flink_user
spring.datasource.password=flink_pass

# Server port (avoid conflict with OpenMRS)
server.port=8081
```

### Step 4: Start Your Flink CDC Application

```bash
./gradlew bootRun
```

### Step 5: Register a CDC Job

Use the provided `docker-vitals-job.yaml`:

```bash
curl -X POST http://localhost:8081/api/jobs/register \
  -H "Content-Type: application/x-yaml" \
  --data-binary @docker-vitals-job.yaml
```

### Step 6: Verify CDC is Working

1. **Check flattened database**:
```bash
mysql -h localhost -P 3308 -u root -prootpass flattened -e "SELECT * FROM vitals;"
```

2. **Create new vitals in OpenMRS** (via web UI)

3. **Watch CDC capture the change**:
```bash
mysql -h localhost -P 3308 -u root -prootpass flattened -e "SELECT * FROM vitals ORDER BY encounter_datetime DESC LIMIT 5;"
```

## Connection Strings Summary

| Service | JDBC URL | Username | Password |
|---------|----------|----------|----------|
| OpenMRS Source | `jdbc:mysql://localhost:3306/openmrs` | `openmrs` | `Admin123` |
| Target/Flattened | `jdbc:mysql://localhost:3308/flattened` | `root` | `rootpass` |
| App Metadata | `jdbc:mysql://localhost:3307/openmrs_flink` | `flink_user` | `flink_pass` |

## Concept IDs for OpenMRS Demo Data

The `docker-vitals-job.yaml` uses standard OpenMRS concept IDs:

| Concept ID | Name |
|------------|------|
| 5085 | Systolic Blood Pressure |
| 5086 | Diastolic Blood Pressure |
| 5088 | Temperature (C) |
| 5089 | Weight (kg) |
| 5090 | Height (cm) |
| 5087 | Pulse |
| 5242 | Respiratory Rate |
| 5092 | Blood Oxygen Saturation |

**Note**: If using a different OpenMRS instance, concept IDs may differ. Query your database:

```sql
SELECT concept_id, name
FROM concept_name
WHERE name LIKE '%blood pressure%'
  AND locale = 'en'
  AND concept_name_type = 'FULLY_SPECIFIED';
```

## Troubleshooting

### OpenMRS won't start
```bash
docker-compose logs openmrs
```
Common issue: Database not ready. Wait for `openmrs-db` health check to pass.

### CDC not capturing changes
1. Check binlog is enabled:
```bash
docker exec openmrs-source-db mysql -u root -pAdmin123 -e "SHOW VARIABLES LIKE 'log_bin';"
```
Should show: `log_bin | ON`

2. Check binlog format:
```bash
docker exec openmrs-source-db mysql -u root -pAdmin123 -e "SHOW VARIABLES LIKE 'binlog_format';"
```
Should show: `binlog_format | ROW`

### Reset Everything
```bash
docker-compose down -v
docker-compose up -d
```

## Stopping Services

```bash
# Stop all services
docker-compose down

# Stop and remove volumes (WARNING: deletes all data)
docker-compose down -v
```

## Development Workflow

1. **Make changes in OpenMRS UI** → CDC captures → Data appears in `flattened.vitals`
2. **Modify YAML job** → Register new version → Old job continues, new job starts
3. **Test different encounter types** → Create jobs for forms, orders, etc.

## Next Steps

- Explore other OpenMRS encounter types (appointments, lab orders, etc.)
- Create additional pivot jobs for different clinical forms
- Add more lookup tables (person, location, users) for data enrichment
- Implement job management endpoints (start/stop/delete)