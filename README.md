# Healthcare Master Data Management (Healthcare_MDM)
## Complete Project Documentation

# Author

**Naresh Mayari**

 Data Engineer | Python | PySpark | Databricks | Snowflake | dbt | AWS | Power BI

- GitHub: https://github.com/naresh-1388
- LinkedIn: https://www.linkedin.com/in/naresh-m-2407t/
**Stack:** Python · AWS S3 · Databricks · Delta Lake · Unity Catalog · Snowflake · dbt · Power BI  
**Architecture:** Medallion (Bronze → Silver → Gold) + Snowflake CDC + dbt Semantic Layer + BI Dashboard  

---
## End-to-End Architecture

![Architecture]()


## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture – End to End](#2-architecture--end-to-end)
3. [Technology Stack](#3-technology-stack)
4. [Project Folder Structure](#4-project-folder-structure)
5. [Layer 0 – Python Extraction Layer](#5-layer-0--python-extraction-layer)
6. [Layer 1 – AWS S3 Storage](#6-layer-1--aws-s3-storage)
7. [Layer 2 – Databricks Bronze Layer](#7-layer-2--databricks-bronze-layer)
8. [Layer 3 – Databricks Silver Layer](#8-layer-3--databricks-silver-layer)
9. [Layer 4 – Databricks Gold Layer (MDM)](#9-layer-4--databricks-gold-layer-mdm)
10. [Databricks Orchestration – Jobs & Pipelines](#10-databricks-orchestration--jobs--pipelines)
11. [Unity Catalog Structure](#11-unity-catalog-structure)
12. [Layer 5 – Snowflake Pipeline](#12-layer-5--snowflake-pipeline)
13. [Layer 6 – dbt Transformation Layer](#13-layer-6--dbt-transformation-layer)
14. [Layer 7 – Power BI Dashboard](#14-layer-7--power-bi-dashboard)
15. [Data Schemas & Models](#15-data-schemas--models)
16. [Key Design Patterns](#16-key-design-patterns)
17. [Batch Data – Real Numbers](#17-batch-data--real-numbers)
18. [Current Build Status](#18-current-build-status)
19. [Industry-Standard Next Steps](#19-industry-standard-next-steps)

---

## 1. Project Overview

Healthcare_MDM is a **production-grade Master Data Management pipeline** built to extract, standardize, deduplicate, and curate US healthcare provider records into a single trusted golden record — and then make that golden record consumable by both SQL analysts (via dbt-modeled marts) and business stakeholders (via a Power BI dashboard).

### What problem does it solve?

In healthcare systems, the same provider often appears across multiple source systems with:
- Slightly different name spellings (John Smith vs. JOHN Q SMITH)
- Multiple addresses from different facilities
- Different credential formats (MD vs M.D.)
- Duplicate entries from system merges/acquisitions

Healthcare MDM solves this by ingesting all provider records, applying matching and survivorship rules, and producing **one trusted golden record per provider** — the single source of truth for downstream systems. That golden record then flows through a dbt semantic layer into an executive Power BI dashboard.

### Data Source

The pipeline pulls live data from the **NPPES NPI Registry** — the official US government database of all registered healthcare providers.

- API: `https://npiregistry.cms.hhs.gov/api/`
- Data: Individual providers (NPI-1 type), filtered by City + State
- Batches run: 3 batches (Baltimore, MD) → **35 total providers** ingested

---

## 2. Architecture – End to End

```
NPPES Registry API (cms.hhs.gov)
        │
        ▼
PYTHON EXTRACTION LAYER (VS Code)
  nppes_extract.py
    ├── GET /api/?city=Baltimore&state=MD
    ├── process_jisb_response.py  →  Standardize
    ├── transform_to_jisb.py      →  JISB Payload
    └── s3_upload.py              →  Upload to S3
        │
        ├──────────────────────────┐
        ▼                         ▼
   S3: raw/                 S3: processed/
   raw API response         JISB Bronze payloads
                                  │
                                  ▼
DATABRICKS (Unity Catalog — healthcare_mdm catalog)
  ┌─────────────────────────────────────────────┐
  │ BRONZE LAYER  healthcare_mdm.bronze.*       │
  │  Auto Loader → provider_bronze Delta table  │
  └─────────────────────┬───────────────────────┘
                        ▼
  ┌─────────────────────────────────────────────┐
  │ SILVER LAYER  healthcare_mdm.silver.*       │
  │  Parse JSON → Explode → Pivot → DQ Clean    │
  └─────────────────────┬───────────────────────┘
                        ▼
  ┌─────────────────────────────────────────────┐
  │ GOLD LAYER   healthcare_mdm.gold.*          │
  │  Business Rules → Matching → Survivorship   │
  │  → Golden Record → Export Parquet to S3     │
  └─────────────────────┬───────────────────────┘
  All 3 chained in 1 Databricks Job (Serverless)
        │
        ▼
   S3: gold/  provider_gold_YYYYMMDD.parquet
        │
        ▼
SNOWFLAKE (ORIEO_DB)
  Storage Integration → ORIEO_STAGE → Snowpipe
  LANDING_SC.PROVIDER_RAW  ← ORIEO_PIPE (AUTO_INGEST)
        │
        ▼
  PROVIDER_RAW_STREAM (CDC)
        │
        ▼
  LOAD_PROVIDER_GOLD_TASK (MERGE ON ProviderID)
        │
        ▼
  GOLD_SC.PROVIDER_GOLD  (1 row per ProviderID)
        │
        ▼
DBT (ANALYTICS_SC)
  PROVIDER_GOLD → stg_provider → dim_provider
                                      │
      ┌──────┬──────┬──────┬──────────┴──────────┐
      ▼      ▼      ▼      ▼                     ▼
   STATE  SUMMARY GENDER TAXONOMY   BUSINESS_RULES / DATA_QUALITY
        │
        ▼
POWER BI DESKTOP
  Import 7 dbt models → Data Model → 5 DAX Measures
  → Executive Healthcare Provider Analytics Dashboard
```

---

## 3. Technology Stack

| Layer | Technology | Purpose |
|---|---|---|
| Data Source | NPPES NPI Registry API | US gov healthcare provider registry |
| Extraction | Python 3 + `requests` | REST API extraction, JSON processing |
| Transformation | Python 3 | JISB payload building, standardization |
| Cloud Storage | AWS S3 (`healthcare-mdm`) | Raw/Processed/Gold file storage |
| Cloud SDK | `boto3` | Python → S3 upload |
| Big Data Processing | PySpark (Databricks) | Distributed data processing |
| Storage Format | Delta Lake | ACID transactions, time travel, schema enforcement |
| Metadata Catalog | Unity Catalog | Governance, lineage, access control |
| Volume Storage | UC Managed Volume | File bridge between S3 and notebooks |
| Orchestration (DB) | Databricks Jobs & Pipelines | Serverless task chain: Bronze → Silver → Gold |
| Data Warehouse | Snowflake | Curated analytical layer |
| Ingestion | Snowflake Snowpipe | Automated S3 → Snowflake loading |
| CDC | Snowflake Stream | Change Data Capture on PROVIDER_RAW |
| Orchestration (SF) | Snowflake Task | Scheduled MERGE operations |
| Transformation | dbt Core | Staging → Dimension → Mart modeling |
| Data Modeling | dbt sources / ref() | Dependency-aware SQL + lineage |
| Testing | dbt tests (schema.yml) | unique, not_null, accepted_values |
| Documentation | dbt docs | Auto-generated lineage graph + model docs |
| BI / Visualization | Power BI Desktop | Executive dashboard, Import mode |
| Semantic Layer | DAX Measures | 5 KPI measures |
| Config Management | `python-dotenv` + `.env` | Secure secrets management |
| Version Control | Git + GitHub | Code versioning |
| IDE | VS Code | Local development |

---

## 4. Project Folder Structure

```
Healthcare_MDM/
├── config/
├── data/
│   ├── processed/
│   │   ├── processed_20260628_182528.json   # Batch 1: 10 providers
│   │   ├── processed_20260628_192429.json   # Batch 2: 10 providers
│   │   └── processed_20260628_200526.json   # Batch 3: 15 providers
│   └── raw/
│       ├── raw_20260628_182528.json
│       ├── raw_20260628_192429.json
│       └── raw_20260628_200526.json
├── databricks/
│   ├── bronze/
│   │   └── bronze_provider_ingestion.py
│   ├── silver/
│   │   └── silver_provider_standardization.py
│   └── gold/
│       └── gold_provider_mdm.py
├── docs/ 
├   ├──  architecture.md
│   ├── source_analysis.md
├   ├── field_mapping.md
├
├── healthcare_mdm_dbt/
│   ├── models/
│   │   ├── sources.yml
│   │   ├── schema.yml
│   │   ├── staging/
│   │   │   └── stg_provider.sql             # VIEW
│   │   ├── dimensions/
│   │   │   └── dim_provider.sql             # TABLE
│   │   └── marts/
│   │       ├── mart_provider_state.sql
│   │       ├── mart_provider_summary.sql
│   │       ├── mart_provider_gender.sql
│   │       ├── mart_provider_taxonomy.sql
│   │       ├── mart_provider_business_rules.sql
│   │       └── mart_provider_data_quality.sql
│   ├── tests/
│   ├── logs/
│   ├── target/                              # compiled SQL, manifest, lineage
│   ├── dbt_project.yml
│   └── profiles.yml                         # Snowflake connection
├── mdmenv/
├── power bi/
│   ├── Healthcare_MDM.pbix
│   └── Screenshots/
│       ├── Dashboard.png
│       └── Model View.png
├── snowflake/
│   ├── 01_Landing/
│   │   ├── 01_Environment.sql
│   │   ├── 02_Landing_Table.sql
│   │   ├── 03_Snowpipe.sql
│   │   └── 04_Monitoring.sql
│   ├── 02_Gold/
│   │   ├── 05_Gold_Table.sql
│   │   ├── 06_Stream.sql
│   │   ├── 07_Task.sql
│   │   └── 08_Gold_Monitoring.sql
│   ├── 03_Analytics/
│   │   └── 09_Analytics.sql                 # 35 business queries
│   ├── 04_Utilities/
│   │   └── Monitoring_Queries.sql
│   └── dbt/
│       ├── Dimensions.sql                   # DIM_PROVIDER validation
│       └── Marts.sql                        # Mart validation + reconciliation
├── src/
│   ├── extract/
│   │   └── nppes_extract.py
│   ├── transform/
│   │   ├── process_jisb_response.py
│   │   └── transform_to_jisb.py
│   └── utils/
│       └── s3_upload.py
├── .env
└── .gitignore
```

---

## 5. Layer 0 – Python Extraction Layer

### 5.1 `nppes_extract.py` — Pipeline Orchestrator

```
1. Load .env config
2. Generate Batch ID:  BATCH_20260628_200526
3. Call NPPES API:     GET /api/?city=Baltimore&state=MD&enumeration_type=NPI-1&limit=25
4. Save Raw JSON:      data/raw/raw_20260628_200526.json
5. Process into JISB:  process_jisb_response() → transform_to_jisb()
6. Wrap with metadata: PROVIDER_JSON, SOURCE_SYSTEM, LOAD_TIMESTAMP, BATCH_ID
7. Save Processed:     data/processed/processed_20260628_200526.json
8. Upload Raw to S3:   s3://healthcare-mdm/raw/
9. Upload Processed:   s3://healthcare-mdm/processed/
10. Print summary
```

### 5.2 `process_jisb_response.py` — NPPES Response Normalizer

Takes nested NPPES JSON and flattens it. Takes only `addresses[0]`, `taxonomies[0]`, `identifiers[0]` as primary entries. Constructs `Full Name` by joining first + middle + last.

**Standardized output per provider (25 flat fields):**
```python
{
    "Provider ID":    "1285195123",
    "Full Name":      "KARA ANGELA ABARCAR",
    "Status":         "A",
    "Gender":         "F",
    "Taxonomy Code":  "207P00000X",
    "License Number": "d93593",
    "State":          "MD",
    "Country Code":   "US",
    "Phone Number":   "410-955-3380",
    # ... 16 more fields
}
```

### 5.3 `transform_to_jisb.py` — JISB Payload Builder

Converts standardized dicts into enterprise JISB format. Each field specifies match method (EXACT or FUZZY) and precision score. Adding new fields requires only one tuple in `JISB_FIELD_MAPPING` — no logic changes needed.

**JISB Field Mapping (25 fields):**

| Source Field | JISB Field | Method | Precision |
|---|---|---|---|
| Provider ID | ProviderID | EXACT | 100 |
| Full Name | FullName | **FUZZY** | **95** |
| Status | Status | EXACT | 100 |
| Address Line 1 | AddressLine1 | **FUZZY** | **90** |
| City | City | **FUZZY** | **90** |
| State | State | EXACT | 100 |
| Taxonomy Code | TaxonomyCode | EXACT | 100 |
| Taxonomy Description | TaxonomyDescription | **FUZZY** | **90** |
| License Number | LicenseNumber | EXACT | 100 |
| + 16 more EXACT fields | ... | EXACT | 100 |

**Output JISB Payload:**
```json
{
  "resultSize": "20",
  "entityType": "Activity",
  "codBases": ["US"],
  "fields": [
    {"method": "EXACT", "name": "ProviderID", "values": ["1285195123"], "fuzzyPrecision": 100},
    {"method": "FUZZY", "name": "FullName", "values": ["KARA ANGELA ABARCAR"], "fuzzyPrecision": 95}
  ]
}
```

### 5.4 Bronze-Ready Processed JSON Structure

```json
[
  {
    "PROVIDER_JSON": "{...full JISB payload as string...}",
    "SOURCE_SYSTEM": "NPPES",
    "PAYLOAD_FORMAT": "JISB",
    "TARGET_SYSTEM": "ORIEO",
    "LOAD_TIMESTAMP": "2026-06-28 20:05:28",
    "LOAD_DATE": "2026-06-28",
    "FILE_NAME": "processed_20260628_200526.json",
    "BATCH_ID": "BATCH_20260628_200526"
  }
]
```

This is exactly the schema of the **Bronze Delta table**.

---

## 6. Layer 1 – AWS S3 Storage

**Bucket:** `healthcare-mdm`

| Folder | Written by | Read by | Content |
|---|---|---|---|
| `raw/` | Python | Reference/Audit | Raw NPPES API responses |
| `processed/` | Python | Databricks Auto Loader | Bronze-ready JISB payloads |
| `gold/` | Databricks Gold | Snowflake Snowpipe | Curated Parquet files |
| `archive/` | Future | Future | Historical files |

S3 is the **central handoff point** — Python writes to it, Databricks reads `processed/` and writes `gold/`, Snowflake reads `gold/`.

---

## 7. Layer 2 – Databricks Bronze Layer

**Notebook:** `bronze_provider_ingestion.py`  
**Table:** `healthcare_mdm.bronze.provider_bronze`

### Auto Loader — Streaming Ingestion

```python
bronze_stream = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(bronze_schema)
        .load("/Volumes/healthcare_mdm/landing/healthcare_files/processed/")
)

bronze_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/.../checkpoint/bronze")
    .trigger(availableNow=True)    # batch mode — process all pending, then stop
    .toTable("healthcare_mdm.bronze.provider_bronze")
```

`trigger(availableNow=True)` processes all pending files and stops — behaves like a batch job, not continuous streaming.

### Bronze Table Schema

| Column | Type | Description |
|---|---|---|
| PROVIDER_JSON | STRING | Complete JISB payload (raw string, untouched) |
| SOURCE_SYSTEM | STRING | Always "NPPES" |
| PAYLOAD_FORMAT | STRING | Always "JISB" |
| TARGET_SYSTEM | STRING | Always "ORIEO" |
| LOAD_TIMESTAMP | TIMESTAMP | Exact time of ingestion |
| LOAD_DATE | DATE | Ingestion date |
| FILE_NAME | STRING | Source file name (lineage) |
| BATCH_ID | STRING | Unique batch identifier |

### Idempotency
- `CREATE TABLE IF NOT EXISTS` — safe to re-run
- Auto Loader checkpoint — same file never re-processed
- `trigger(availableNow=True)` — only processes pending files

---

## 8. Layer 3 – Databricks Silver Layer

**Notebook:** `silver_provider_standardization.py`  
**Table:** `healthcare_mdm.silver.provider_standardized`

### 9-Step Processing

```
1. Incremental batch filter — skip already-processed BATCH_IDs
2. Define JISB payload schema (StructType with nested fields[])
3. Parse PROVIDER_JSON → from_json() → typed Spark struct
4. Explode fields[] array → 1 row per provider → N rows (1 per field)
5. Flatten → field.name + field.values[0] + match_method + fuzzy_precision
6. Pivot on FIELD_NAME → collapse N rows back to 1 wide row per provider
7. DQ — dynamically trim() all strings, empty "" → NULL (no hardcoded columns)
8. Null % report per column (informational only, no records rejected)
9. Append to Silver Delta table
```

### Silver Table Schema

| Category | Columns |
|---|---|
| Identity | ProviderID, FirstName, MiddleName, LastName, FullName |
| Credentials | Credential, Gender, Status |
| Dates | EnumerationDate, LastUpdated |
| Address | AddressLine1, City, State, PostalCode, CountryCode, CountryName |
| Contact | PhoneNumber, FaxNumber |
| Taxonomy | TaxonomyCode, TaxonomyDescription, LicenseNumber, LicenseState |
| Identifier | Identifier, IdentifierIssuer, IdentifierType |
| Metadata | SOURCE_SYSTEM, PAYLOAD_FORMAT, TARGET_SYSTEM, LOAD_TIMESTAMP, LOAD_DATE, FILE_NAME, BATCH_ID |

---

## 9. Layer 4 – Databricks Gold Layer (MDM)

**Notebook:** `gold_provider_mdm.py`  
**Output:** `healthcare_mdm.gold.provider_gold` + `healthcare_mdm.gold.export_log`

### Business Rules Validation (BR-001 to BR-004)

```python
gold_df = gold_df.withColumn(
    "BUSINESS_RULE_STATUS",
    when(col("ProviderID").isNull(),    "FAIL")   # BR-001: ProviderID mandatory
    .when(col("Status") != "A",         "FAIL")   # BR-002: Must be Active
    .when(col("TaxonomyCode").isNull(), "FAIL")   # BR-003: TaxonomyCode mandatory
    .when(col("CountryCode") != "US",   "FAIL")   # BR-004: US providers only
    .otherwise("PASS")
)
```

> Records that FAIL are **NOT deleted** — they are flagged and passed downstream for review.

### Data Quality Warnings (DQ-001 to DQ-003)

```python
gold_df = gold_df.withColumn(
    "DATA_QUALITY_WARNING",
    when(col("PhoneNumber").isNull(),    "PhoneNumber Missing")
    .when(col("LicenseNumber").isNull(), "LicenseNumber Missing")
    .when(col("Identifier").isNull(),    "Identifier Missing")
    .otherwise("No Data Quality Issues")
)
```

### Matching & Survivorship

**Exact Match:** `Window.partitionBy("ProviderID")` → `EXACT_MATCH_STATUS = MATCH / UNIQUE`

**Fuzzy Match:** `concat_ws("|", PhoneNumber, PostalCode)` as key → `FUZZY_MATCH_STATUS = REVIEW / UNIQUE`

**Survivorship (SR-001 + SR-002):**
```python
survivorship_window = (
    Window.partitionBy("ProviderID")
          .orderBy(col("Status").desc(), col("LastUpdatedDate").desc())
)
gold_df = gold_df.withColumn("SURVIVOR_RANK", row_number().over(survivorship_window))
golden_df = gold_df.filter(col("SURVIVOR_RANK") == 1)
```

**Example:**
```
ProviderID 1234567890:
  Record A: Status=A, LastUpdated=2023-01-15 → RANK 1 (SURVIVOR) ✓
  Record B: Status=I, LastUpdated=2023-01-20 → RANK 2 (DISCARDED) ✗
  Record C: Status=A, LastUpdated=2023-01-10 → RANK 3 (DISCARDED) ✗
```

### ORIEO Payload — 23 Columns

```python
orieo_df = golden_df.select(
    # 14 business columns
    "ProviderID", "FullName", "Credential", "Gender", "Status",
    "AddressLine1", "City", "State", "PostalCode", "CountryCode",
    "PhoneNumber", "TaxonomyCode", "LicenseNumber", "Identifier",
    # 2 MDM flags
    "BUSINESS_RULE_STATUS", "DATA_QUALITY_WARNING",
    # 7 audit columns
    "SOURCE_SYSTEM", "PAYLOAD_FORMAT", "TARGET_SYSTEM",
    "LOAD_TIMESTAMP", "LOAD_DATE", "FILE_NAME", "BATCH_ID"
)
```

All MDM processing columns (SURVIVOR_RANK, FUZZY_MATCH_KEY etc.) are **internal only** — stripped before output.

### Export to S3 via UC Volume

```python
export_path = "/Volumes/healthcare_mdm/landing/healthcare_files/gold/provider_gold_{ts}.parquet"
export_df.coalesce(1).write.mode("overwrite").parquet(export_path)
```

`coalesce(1)` produces a single Parquet file per batch — clean for Snowpipe tracking.

**Export Log** (per attempt): EXPORT_ID (UUID), BATCH_ID, EXPORT_TIMESTAMP, S3_PATH, ROW_COUNT, STATUS, ERROR_MESSAGE

---

## 10. Databricks Orchestration – Jobs & Pipelines

Once the 3 notebooks were individually validated, they were chained into a single **Databricks Job**.

### Job Structure

```
Healthcare_MDM (Job — ID: 721161475215388)
  │
  ├── Task: Bronze   → bronze/bronze_provider_ingestion   (Serverless)
  │         ↓ (only runs if Bronze succeeds)
  ├── Task: Silver   → silver/silver_provider_standardization  (Serverless)
  │         ↓ (only runs if Silver succeeds)
  └── Task: Gold     → gold/gold_provider_mdm              (Serverless)
```

- **Compute:** Serverless — no manual cluster sizing or provisioning needed
- **Lineage:** 4 upstream tables, 4 downstream tables — auto-tracked by Unity Catalog
- **Performance optimized:** On
- **Trigger:** Manual "Run now" today; one click to schedule as daily cron

### Why This Matters

Before the Job, validating the pipeline meant opening 3 notebooks manually and waiting. With the Job:
- One **"Run now"** click runs Bronze → Silver → Gold in sequence
- Bronze fail → Silver and Gold never run (no silent partial-pipeline corruption)
- Job is ready to be put on a **cron schedule** for production use

### Trigger Options

| Trigger | Status | Use Case |
|---|---|---|
| Manual ("Run now") | **Active today** | Testing, demos, on-demand batch |
| Scheduled (cron) | Ready to enable | Daily 6 AM refresh in production |
| File arrival trigger | Future upgrade | Auto-run when new file lands in S3 |

---

## 11. Unity Catalog Structure

A single `healthcare_mdm` catalog organizes all of Databricks — schemas, tables, and the managed Volume that bridges S3 and the notebooks.

### Catalog Layout

```
healthcare_mdm (Catalog)
├── bronze              (Schema)
│     └── provider_bronze           ← Delta table
├── silver               (Schema)
│     └── provider_standardized     ← Delta table
├── gold                 (Schema)
│     ├── provider_gold             ← Delta table (golden records)
│     └── export_log                ← Delta table (export audit)
├── landing              (Schema)
│     └── Volumes (1)
│           └── healthcare_files   (Managed Volume)
│                 ├── checkpoint/   ← Auto Loader checkpoints
│                 ├── gold/         ← Gold Parquet exports (→ S3 gold/)
│                 ├── processed/    ← Bronze-ready JSON (← S3 processed/)
│                 └── raw/          ← Raw NPPES JSON (← S3 raw/)
├── default
└── information_schema
```

### Workspace Notebook Tree

```
Healthcare_MDM (Workspace folder)
├── bronze/
│     └── bronze_provider_ingestion
├── gold/
│     └── gold_provider_mdm
└── silver/
      └── silver_provider_standardization
```

### Design Decisions

| Choice | Reason |
|---|---|
| One catalog (`healthcare_mdm`) | All governance, access control, and lineage under one root |
| Separate schema per layer | Mirrors Bronze/Silver/Gold — clean ACL boundaries possible later |
| `landing` schema hosts the Volume | Volume is the filesystem bridge, not business data |
| Managed Volume (not external table) | Databricks manages storage lifecycle; backed by S3 via External Location |

---

## 12. Layer 5 – Snowflake Pipeline

**Database:** `ORIEO_DB`  
**Schemas:** `LANDING_SC`, `GOLD_SC`, `ANALYTICS_SC`  
**Warehouse:** `ORIEO_WH` (XSMALL, auto-suspend 60s, auto-resume)

### Environment Setup (`01_Environment.sql`)

```sql
CREATE DATABASE Orieo_DB;
CREATE SCHEMA IF NOT EXISTS LANDING_SC;   -- Raw from Snowpipe
CREATE SCHEMA IF NOT EXISTS GOLD_SC;      -- MERGE output
CREATE SCHEMA IF NOT EXISTS ANALYTICS_SC; -- dbt models

CREATE WAREHOUSE Orieo_WH
WITH WAREHOUSE_SIZE='XSMALL' AUTO_SUSPEND=60 AUTO_RESUME=TRUE;

CREATE STORAGE INTEGRATION Orieo_integration
  TYPE=EXTERNAL_STAGE  STORAGE_PROVIDER='S3'  ENABLED=TRUE
  STORAGE_AWS_ROLE_ARN='AWS ARN'
  STORAGE_ALLOWED_LOCATIONS=('s3://healthcare-mdm/gold/');

CREATE FILE FORMAT Orieo_FF TYPE=PARQUET;

CREATE STAGE Orieo_stage
  STORAGE_INTEGRATION=Orieo_integration
  URL='s3://healthcare-mdm/gold/'
  FILE_FORMAT=Orieo_FF;
```

> After creating the storage integration, run `DESC STORAGE INTEGRATION Orieo_integration` — copy `STORAGE_AWS_IAM_USER_ARN` + `STORAGE_AWS_EXTERNAL_ID` into the AWS IAM Role Trust Policy.

### Snowpipe (`03_Snowpipe.sql`)

```sql
CREATE OR REPLACE PIPE ORIEO_PIPE
  AUTO_INGEST = TRUE
AS
  COPY INTO PROVIDER_RAW
  FROM @ORIEO_STAGE
  FILE_FORMAT = (FORMAT_NAME = ORIEO_FF)
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE  -- Parquet col names match table col names
  ON_ERROR = CONTINUE;                     -- bad rows skip, valid rows still load
```

**How AUTO_INGEST works:** S3 sends SQS event notification → Snowpipe receives it → COPY INTO runs automatically → no manual trigger needed.

### Snowflake Stream (CDC)

```sql
CREATE STREAM LANDING_SC.PROVIDER_RAW_STREAM
ON TABLE LANDING_SC.PROVIDER_RAW;

-- Check if stream has new data
SELECT SYSTEM$STREAM_HAS_DATA('PROVIDER_RAW_STREAM');
```

Tracks every INSERT into PROVIDER_RAW since last consumed. Pointer advances once the Task consumes the stream — records not re-read.

### Snowflake Task (MERGE)

```sql
CREATE OR REPLACE TASK LOAD_PROVIDER_GOLD_TASK
  WAREHOUSE = ORIEO_WH
  WHEN SYSTEM$STREAM_HAS_DATA('PROVIDER_RAW_STREAM')
AS
  MERGE INTO GOLD_SC.PROVIDER_GOLD T
  USING LANDING_SC.PROVIDER_RAW_STREAM S
  ON T.ProviderID = S.ProviderID

  WHEN MATCHED THEN
    UPDATE SET T.FullName=S.FullName, T.Status=S.Status, ...  -- 22 columns

  WHEN NOT MATCHED THEN
    INSERT (ProviderID, FullName, ...) VALUES (S.ProviderID, S.FullName, ...);
```

**Result:** `PROVIDER_GOLD` always has exactly **one record per ProviderID**.

### Analytics Layer (`09_Analytics.sql`)

35 business analytics queries in 3 parts:

**Part 1A (Q1–Q9):** Total providers, Active/Inactive, by State/Country/Gender/Credential/City, Business Rule summary, DQ Warning summary

**Part 1B (Q10–Q18):** Latest batch, by Source System/Payload/Target, recent records, duplicate check, missing Phone/License/Taxonomy

**Part 2 (Q19–Q35):** Full listing, records per batch, Taxonomy/Postal code distribution, file-wise counts, daily monitoring, distinct States/Cities/Credentials/Taxonomies, BR/DQ by State, by State+Gender

---

## 13. Layer 6 – dbt Transformation Layer

**Project:** `healthcare_mdm_dbt`  
**Source:** `ORIEO_DB.GOLD_SC.PROVIDER_GOLD`  
**Target:** `ANALYTICS_SC`

### Purpose

dbt sits on top of `GOLD_SC.PROVIDER_GOLD` and builds a clean staging → dimension → mart structure, giving Power BI (and any future BI tool) a stable, tested semantic layer. This also closes the loop on the `ANALYTICS_SC` schema created in `01_Environment.sql` but left empty until now.

### Connection Verified

`profiles.yml` configured with: Account, User, Password, Warehouse (`ORIEO_WH`), Database (`ORIEO_DB`), Schema (`ANALYTICS_SC`), Role (`ACCOUNTADMIN`)

```
dbt debug → All checks passed ✅
```

### Models

| Model | Materialization | Output | Purpose |
|---|---|---|---|
| `stg_provider.sql` | **VIEW** | _(staging)_ | First transformation layer — reads from Gold source |
| `dim_provider.sql` | **TABLE** | `ANALYTICS_SC.DIM_PROVIDER` | Master provider dimension, 23 columns |
| `mart_provider_state.sql` | **TABLE** | `ANALYTICS_SC.MART_PROVIDER_STATE` | State-wise provider summary |
| `mart_provider_summary.sql` | **TABLE** | `ANALYTICS_SC.MART_PROVIDER_SUMMARY` | Daily load date summary |
| `mart_provider_gender.sql` | **TABLE** | `ANALYTICS_SC.MART_PROVIDER_GENDER` | Gender distribution |
| `mart_provider_taxonomy.sql` | **TABLE** | `ANALYTICS_SC.MART_PROVIDER_TAXONOMY` | Taxonomy distribution |
| `mart_provider_business_rules.sql` | **TABLE** | `ANALYTICS_SC.MART_PROVIDER_BUSINESS_RULES` | BR PASS/FAIL monitoring |
| `mart_provider_data_quality.sql` | **TABLE** | `ANALYTICS_SC.MART_PROVIDER_DATA_QUALITY` | DQ warning monitoring |

### dim_provider.sql

```sql
{{ config(materialized='table') }}

SELECT
ProviderID, FullName, Credential, Gender, Status,
AddressLine1, City, State, PostalCode, CountryCode,
PhoneNumber, TaxonomyCode, LicenseNumber, Identifier,
BUSINESS_RULE_STATUS, DATA_QUALITY_WARNING,
SOURCE_SYSTEM, PAYLOAD_FORMAT, TARGET_SYSTEM,
LOAD_TIMESTAMP, LOAD_DATE, FILE_NAME, BATCH_ID
FROM {{ source('gold','provider_gold') }}
```

### mart_provider_state.sql

```sql
{{ config(materialized='table') }}

SELECT
State,
COUNT(*) AS TOTAL_PROVIDERS,
SUM(CASE WHEN Status='Active' THEN 1 ELSE 0 END)   AS ACTIVE_PROVIDERS,
SUM(CASE WHEN Status='Inactive' THEN 1 ELSE 0 END) AS INACTIVE_PROVIDERS,
SUM(CASE WHEN Gender='Male' THEN 1 ELSE 0 END)     AS MALE_PROVIDERS,
SUM(CASE WHEN Gender='Female' THEN 1 ELSE 0 END)   AS FEMALE_PROVIDERS,
COUNT(DISTINCT TaxonomyCode) AS TOTAL_SPECIALIZATIONS
FROM {{ ref('dim_provider') }}
GROUP BY State
ORDER BY TOTAL_PROVIDERS DESC
```

### dbt Commands Executed

```bash
dbt --version
dbt debug
dbt parse
dbt run
dbt run --select stg_provider
dbt run --select dim_provider
dbt run --select mart_provider_state
dbt run --select mart_provider_summary
dbt run --select mart_provider_gender
dbt run --select mart_provider_taxonomy
dbt run --select mart_provider_business_rules
dbt run --select mart_provider_data_quality
dbt test
dbt docs generate
dbt docs serve
```

### dbt Tests (`schema.yml`)

| Column | Test | Result |
|---|---|---|
| ProviderID | `unique` | ✅ PASS |
| ProviderID | `not_null` | ✅ PASS |
| TaxonomyCode | `not_null` | ✅ PASS |
| CountryCode | `accepted_values` | ✅ PASS |

Two issues resolved: `accepted_values` syntax updated to dbt 1.11 `arguments` format; CountryCode values corrected to match real Gold data. **All tests passed.**

### dbt Lineage

```
PROVIDER_GOLD
      │
      ▼
  STG_PROVIDER
      │
      ▼
  DIM_PROVIDER
      │
  ┌───┼────┬──────┬────────────────┬───────────────┐
  ▼   ▼    ▼      ▼                ▼               ▼
STATE SUMMARY GENDER TAXONOMY BUSINESS_RULES DATA_QUALITY
```

`dbt docs generate` + `dbt docs serve` → interactive lineage graph with model/column docs.

### Validation Queries (Snowflake `dbt/Dimensions.sql` + `dbt/Marts.sql`)

```sql
-- Gold vs Dimension row count match
SELECT
(SELECT COUNT(*) FROM ORIEO_DB.GOLD_SC.PROVIDER_GOLD) AS GOLD_COUNT,
(SELECT COUNT(*) FROM DIM_PROVIDER) AS DIM_COUNT;

-- Duplicate ProviderID check on dimension
SELECT ProviderID, COUNT(*)
FROM DIM_PROVIDER
GROUP BY ProviderID
HAVING COUNT(*) > 1;
```

---

## 14. Layer 7 – Power BI Dashboard

**File:** `power bi/Healthcare_MDM.pbix`  
**Source:** Snowflake `ORIEO_DB.ANALYTICS_SC` (7 dbt models, Import mode)

### Connection Setup

| Setting | Value |
|---|---|
| Software | Power BI Desktop |
| Server | `<account_locator>.snowflakecomputing.com` |
| Warehouse | ORIEO_WH |
| Database | ORIEO_DB |
| Schema | ANALYTICS_SC |
| Connection Mode | **Import** |

**Why Import over DirectQuery:** faster performance, best for demo/portfolio scale datasets. Manual refresh is the trade-off — acceptable here.

> When pushing to GitHub: mask the real account locator + username. Do not commit literal Snowflake connection strings.

### Imported Tables (7 dbt models)

```
DIM_PROVIDER                  MART_PROVIDER_STATE
MART_PROVIDER_SUMMARY         MART_PROVIDER_GENDER
MART_PROVIDER_TAXONOMY        MART_PROVIDER_BUSINESS_RULES
MART_PROVIDER_DATA_QUALITY
```

Power Query confirmed names, columns, data types — **no transformations needed** (dbt shaped the data correctly on the way in).

### Data Model — Relationships

| Relationship | Decision | Reason |
|---|---|---|
| DIM_PROVIDER → MART_PROVIDER_STATE | **KEPT** | Slicer-driven cross-filtering needed |
| DIM_PROVIDER → MART_PROVIDER_GENDER | **KEPT** | Slicer-driven cross-filtering needed |
| DIM_PROVIDER → MART_PROVIDER_TAXONOMY | **KEPT** | Slicer-driven cross-filtering needed |
| DIM_PROVIDER → MART_PROVIDER_SUMMARY | **REMOVED** | Pre-aggregated mart — no relationship needed |
| DIM_PROVIDER → MART_PROVIDER_BUSINESS_RULES | **REMOVED** | Pre-aggregated mart — no relationship needed |
| DIM_PROVIDER → MART_PROVIDER_DATA_QUALITY | **REMOVED** | Pre-aggregated mart — no relationship needed |

### DAX Measures (5)

```dax
Total Providers      = COUNTROWS(DIM_PROVIDER)
Total States         = DISTINCTCOUNT(DIM_PROVIDER[State])
Total Cities         = DISTINCTCOUNT(DIM_PROVIDER[City])
Total Specializations = DISTINCTCOUNT(DIM_PROVIDER[TaxonomyCode])
Business Rule Pass   = SUM(MART_PROVIDER_BUSINESS_RULES[TOTAL_PROVIDERS])
```

### Dashboard — Executive Overview

**4 KPI Cards:**

| KPI | Result |
|---|---|
| Total Providers | 35 |
| Total States | 3 (MD, TN, VA) |
| Total Cities | 5 |
| Total Specializations | 22 |

**Slicers:** State (MD, TN, VA) · Gender (F, M)
*(Taxonomy slicer removed — too many distinct values, hurt usability)*

**4 Visuals:**

| # | Visual | Chart Type | Source |
|---|---|---|---|
| 1 | Providers by State | Clustered Bar | MART_PROVIDER_STATE |
| 2 | Gender Distribution | Pie | MART_PROVIDER_GENDER |
| 3 | Data Quality Dashboard | Donut | MART_PROVIDER_DATA_QUALITY |
| 4 | Top 10 Taxonomies | Clustered Bar | MART_PROVIDER_TAXONOMY |

**Business Rule Visual — Design Pivot:**
Initially a Donut chart. Dataset contained only `PASS` records — a single-category donut communicates nothing. Replaced with a **Card** showing "Business Rule PASS = 35". Correct call: let the actual data shape the visual.

### Dashboard Layout

```
─────────────────────────────────────────────────────
Healthcare Provider Analytics Dashboard
─────────────────────────────────────────────────────
[Total Providers] [Total States] [Total Cities] [Specializations]
─────────────────────────────────────────────────────
[State Filter] [Gender Filter]
─────────────────────────────────────────────────────
Providers by State          |    Gender Distribution
─────────────────────────────────────────────────────
Top Taxonomies              |    Data Quality Issues
─────────────────────────────────────────────────────
Business Rule PASS (35)
─────────────────────────────────────────────────────
```

---

## 15. Data Schemas & Models

### Full Data Flow (Column Lineage)

```
NPPES API → Standardized Dict (25 fields)
         → JISB Payload (fields[])
         → Bronze-Ready JSON (PROVIDER_JSON + 7 audit)
         → Bronze Delta (Auto Loader)
         → Silver Delta (from_json + explode + pivot → 32 cols)
         → Gold Delta (23 cols = 14 business + 2 MDM flags + 7 audit)
         → UC Volume gold/ → S3 gold/
         → Snowpipe → PROVIDER_RAW (23 cols)
         → Stream → Task MERGE → PROVIDER_GOLD (23 cols, 1/ProviderID)
         → dbt source() → DIM_PROVIDER (23 cols)
         → dbt ref() → 6 Marts (aggregated)
         → Power BI Import → DAX Measures → Dashboard
```

### Column Journey

| Column | Python | Bronze | Silver | Gold (DB) | Gold (SF) | DIM_PROVIDER |
|---|---|---|---|---|---|---|
| ProviderID | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| FullName | ✓ | ✓ | ✓ | ✓ auto-fill | ✓ | ✓ |
| BUSINESS_RULE_STATUS | — | — | — | ✓ added | ✓ | ✓ |
| DATA_QUALITY_WARNING | — | — | — | ✓ added | ✓ | ✓ |
| BATCH_ID | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| PROVIDER_JSON | ✓ | ✓ | ✓ (key) | — | — | — |
| SURVIVOR_RANK | — | — | — | ✓ internal | — | — |

---

## 16. Key Design Patterns

### 16.1 Medallion Architecture

```
Bronze → immutable, append-only raw store
Silver → clean, standardized, no business logic
Gold   → MDM-processed, serve-ready, export-ready
```

### 16.2 Layered Semantic Modeling (dbt)

```
stg_provider (VIEW, disposable)
    → dim_provider (TABLE, single source of truth)
        → 6 Marts (purpose-built aggregates)
```

Change Gold's schema once → `ref()` dependency graph shows exactly what breaks downstream, instead of manual auditing every report query.

### 16.3 Idempotency at Every Layer

| Layer | Mechanism |
|---|---|
| Python | Unique timestamps → unique file names |
| S3 | Unique file names → no overwrites |
| Bronze | Auto Loader checkpoint |
| Silver | Batch-level BATCH_ID filter |
| Gold | Dual check: batch + ProviderID existence |
| Export | Export log SUCCESS check |
| Snowflake | MERGE ON ProviderID (UPSERT) |
| dbt | `ref()`/`source()` graph → rebuilds only what changed |

### 16.4 MDM Concepts Applied

| MDM Concept | Implementation |
|---|---|
| Matching | Exact (ProviderID) + Fuzzy (Phone+PostalCode) |
| Deduplication | DUPLICATE_STATUS + Survivorship filter |
| Survivorship | Active first → most recent LastUpdated |
| Golden Record | filter(SURVIVOR_RANK == 1) → 1 per NPI |
| Lineage | BATCH_ID + FILE_NAME + LOAD_TIMESTAMP through all 9 layers + dbt lineage graph |
| Audit Log | export_log Delta table with UUID per export |

### 16.5 Orchestration Layering

```
Databricks Job   → orchestrates Bronze → Silver → Gold (Databricks-internal)
Snowflake Task   → orchestrates Stream → Gold MERGE (Snowflake-internal)
dbt run          → orchestrates stg → dim → marts (dbt-internal)
[Future: Airflow] → chains ALL of the above cross-platform
```

### 16.6 Configuration-Driven JISB

```python
("New Field", "NewJISBField", "EXACT", 100)  # just add one tuple — zero logic changes
```

### 16.7 Import vs DirectQuery (Power BI)

Import chosen for performance at demo scale. Trade-off: manual/scheduled refresh instead of live Snowflake queries. Revisit if this scales to production volumes.

---

## 17. Batch Data – Real Numbers

| Batch | File | Providers | Time |
|---|---|---|---|
| BATCH_20260628_182528 | processed_20260628_182528.json | 10 | 18:25:28 |
| BATCH_20260628_192429 | processed_20260628_192429.json | 10 | 19:24:29 |
| BATCH_20260628_200526 | processed_20260628_200526.json | 15 | 20:05:26 |
| **Total** | | **35** | |

**Confirmed in Power BI:** 35 Providers · 3 States (MD, TN, VA) · 5 Cities · 22 Specializations · 35 Business Rule PASS (100%)

**Sample Provider:**
- NPI: `1285195123` · Name: `KARA ANGELA ABARCAR`
- Specialty: Emergency Medicine (`207P00000X`) · License: `d93593` (MD)
- Location: 1800 Orleans St, Baltimore MD · Phone: `410-955-3380`
- Status: Active (`A`) · Gender: Female (`F`)

---

## 18. Current Build Status

| Phase | Component | Status |
|---|---|---|
| **Python** | NPPES API Extraction | ✅ |
| | JISB Transformation | ✅ |
| | AWS S3 Upload | ✅ |
| **Databricks** | Bronze Layer (Auto Loader) | ✅ |
| | Silver Layer (Standardized) | ✅ |
| | Gold Layer (MDM + Survivorship) | ✅ |
| | Databricks Job (orchestrated) | ✅ |
| | Unity Catalog (schemas + Volume) | ✅ |
| **Snowflake** | Storage Integration + Stage | ✅ |
| | Snowpipe (auto-ingest) | ✅ |
| | Stream (CDC) + Task (MERGE) | ✅ |
| | Gold Table + Monitoring SQL | ✅ |
| | Analytics SQL (35 queries) | ✅ |
| **dbt** | Source + Staging (VIEW) | ✅ |
| | Dimension (DIM_PROVIDER, TABLE) | ✅ |
| | 6 Reporting Marts | ✅ |
| | dbt Tests (all passed) | ✅ |
| | dbt Docs + Lineage Graph | ✅ |
| **Power BI** | Snowflake Connection (Import) | ✅ |
| | Data Model (relationships) | ✅ |
| | 5 DAX Measures | ✅ |
| | Executive Overview Dashboard | ✅ |
| **GitHub** | README.md | ✅ |
| | GitHub Repository | ✅ |
