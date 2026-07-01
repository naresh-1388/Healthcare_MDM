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
