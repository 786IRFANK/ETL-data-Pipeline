# NHANES ETL Data Pipeline

## Introduction

This project implements a **scalable, asynchronous ETL data pipeline for NHANES datasets** using the **Medallion Architecture (Bronze → Silver → Gold)**. The pipeline is designed for reliability, observability, and extensibility, making it suitable for both batch and scheduled data ingestion workflows.

The system leverages **FastAPI** for API orchestration, **Celery + Redis** for distributed background processing, **PostgreSQL** for analytical storage, and **Google Cloud Storage (GCS)** as the raw data lake.

---

## Medallion Architecture Overview

###  Bronze Layer – Raw Ingestion
- Source: NHANES datasets (CSV files)
- Storage: Google Cloud Storage (GCS)
- Characteristics:
  - Raw, unmodified data
  - Source-aligned naming
  - Immutable storage

###  Silver Layer – Clean & Standardized
- Source: Bronze layer (GCS CSV files)
- Storage: PostgreSQL (`nhanes_silver` schema)
- Characteristics:
  - Data cleaning & validation
  - Schema normalization
  - One table per NHANES domain

###  Gold Layer – Analytics Ready
- Source: Silver layer tables
- Storage: PostgreSQL (`nhanes_gold` schema)
- Characteristics:
  - Domain-driven aggregations
  - Merged datasets
  - Optimized for reporting & analytics

---

## NHANES Domain Coverage

The pipeline currently supports the following NHANES components:

- **Demographics**
- **Dietary**
- **Examination**
- **Laboratory**
- **Questionnaire**

Each domain follows the same Bronze → Silver → Gold processing flow.

---

## Tech Stack

- **Python 3**
- **FastAPI** – API orchestration layer
- **Celery** – Asynchronous background task processing
- **Redis** – Message broker & result backend
- **PostgreSQL** – Analytical data storage
- **Google Cloud Storage (GCS)** – Raw data lake
- **SQLAlchemy** – ORM & database abstraction
- **Pandas** – Data transformation & merging
- **Alembic** – Database migrations

---

## Project Setup

### 1. Create Virtual Environment

```bash
python -m venv env
env\Scripts\activate
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

---

## Environment Configuration

Create a `.env` file in the project root:

```env
HOST=0.0.0.0
PORT=8000
ENVIRONMENT=development

# Redis Configuration
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_URL=redis://localhost:6379/0

# PostgreSQL Configuration
POSTGRES_HOST=
POSTGRES_PORT=
POSTGRES_USER=
POSTGRES_PASSWORD=
POSTGRES_DB=

# GCP Storage Configuration
GCP_PROJECT_ID=your-project-id
GCP_BUCKET_NAME=
GCP_CREDENTIALS_PATH=

# NHANES Configuration
NHANES_BASE_URL=https://wwwn.cdc.gov
NHANES_YEARS=

# CORS
CORS_ORIGINS=["http://localhost:3000"]
```

---

## Database Migrations (Alembic)

Generate a new migration:

```bash
alembic revision --autogenerate -m "add column"
```

Apply migrations:

```bash
alembic upgrade head
```

---

## Running the Application

### Start FastAPI Server

```bash
uvicorn main:app --reload
```

### Start Celery Worker

```bash
celery -A celery_app.celery_app worker --loglevel=info --pool=prefork --concurrency=4

```

> **Note:** Redis must be running before starting the Celery worker.

---

## Bronze Layer Configuration

The GCS path for Bronze CSV files is defined in:

```
app/config/settings.py
```

```python
BRONZE_PATH: str = "bronze/nhanes"
```

All raw NHANES CSV files are stored under this prefix in the GCS bucket.

---

## API Triggering

The pipeline can be executed **immediately** or **scheduled for later execution**.

### Run Immediately (No Scheduling)

```json
{
  "pipeline_code": "nhanes",
  "run_type": "now",
  "schedule_at": "string",
  "params": {
    "additionalProp1": {}
  }
}
```

### Scheduled Execution

```json
{
  "pipeline_code": "nhanes",
  "run_type": "scheduled",
  "schedule_at": "2026-01-09T05:45:00Z",
  "params": {
    "additionalProp1": {}
  }
}
```

> `schedule_at` **must be in UTC ISO-8601 format**.

---

## UTC Time Validation Example

```python
from datetime import datetime, timezone

utc_time = datetime.now(timezone.utc)
print("UTC Time:", utc_time.strftime("%Y-%m-%d %H:%M:%S"))
```

---

## Silver Layer Processing

- Reads Bronze CSV files from GCS
- Applies data validation & standardization
- Creates tables under the `nhanes_silver` schema
- One table per NHANES component

---

## Gold Layer Logic

### Configuration

Gold table definitions are maintained in:

```
app/config/merge_config.py
```

### Processing Steps

1. Read table definitions from `merge_config.py`
2. Fetch required tables from `nhanes_silver` schema
3. Merge datasets using Pandas (typically on `SEQN`)
4. Persist merged output into `nhanes_gold` schema

This approach allows **configuration-driven Gold table creation** without modifying pipeline logic.

---
---
# Forian Claims Data Pipeline

## API Run Parameters
```json
{
  "pipeline_code": "forian_claims",
  "run_type": "now",
  "schedule_at": "string",
  "params": {
    "additionalProp1": {}
  }
}
```

---

## Project Structure

**Configuration File:**
- `app/config/setting.py`

**Defined Paths:**
- **Landing Path**  
  `FORIAN_CLAIMS_LANDING_PATH = "landing/forian_claims"`

- **Bronze Path**  
  `FORIAN_CLAIMS_BRONZE_PATH = "test/bronze/forian/forian_claims"`

- **Silver Layer**  
  Schema definitions for `FORIAN_CLAIMS_SILVER` are created and maintained here.

---

## Data Pipeline Layers

### 1. Landing Layer
- Raw Forian Claims files are ingested into the **Landing GCS bucket**.

### 2. Bronze Layer
- Data is moved from Landing to **Bronze GCS bucket**.  


### 3. Silver Layer
- Curated datasets with well-defined schemas.  

---

## Silver Layer Schema

The **forian_claims_silver** 

---
---

# ADHOC File Upload Service Overview

The Adhoc Upload Service allows users to upload structured files (CSV or
Excel) which are then processed and stored as database tables in the
axonal_structure_db database.

Each uploaded file is associated with an organization, and the
organization ID is used as the schema name in the database.

### This service supports:

1. User and organization creation
2. User status management
3. Structured file uploads (CSV, Excel)
4. Table creation inside organization-specific schemas

### Architecture Notes

1. Database Used: axonal_structure_db
2. Schema Naming: The organization_id is used as the database schema name.
3. Schema Creation: Schemas are not created automatically by the
application. They must be created manually by the database administrator
before file uploads.

### API Endpoints 

1. **Defined Paths:**
 
Creates a new user and assigns an organization ID.

Endpoint POST /api/v1/adhoc-user/create Example URL
```bash
http://127.0.0.1:8000/api/v1/adhoc-user/
```

**create Description**

1. Creates a new user
2. Generates or assigns an organization ID
3. The organization_id will be used as the schema name in the database

2.  **Defined Paths:**

Updates the status of a user.

Endpoint POST /api/v1/adhoc-user/update-status Example URL
```bash
http://127.0.0.1:8000/api/v1/adhoc-user/update-status 
```

**Description**

1. Users can be marked as:
```bash
Active
Inactive
```

2. Important Rule
If a user's status is Inactive, they cannot access or upload data.

3.  **Defined Paths:**

Uploads structured files (CSV or Excel) and creates database tables.

Endpoint POST /api/v1/adhoc/ Example URL
```bash
http://127.0.0.1:8000/api/v1/adhoc/ 
```

1. Supported File Types = .csv, .xlsx
2. Upload Limits Limit Type Value Maximum Files 5 files per request Total
Size Limit 200 MB (combined size of all files) Processing Workflow
3. User uploads CSV or Excel files.
