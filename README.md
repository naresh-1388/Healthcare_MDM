# Healthcare_MDM Project Progress Documentation

## Project Name

Healthcare_MDM

---

# Project Objective

The objective of this project is to build an enterprise Healthcare Master Data Management (MDM) solution using a real healthcare provider data source.

Instead of using an internal enterprise API like JSIB, we are using the public NPPES Provider Registry API as our source system.

The project follows an enterprise Data Engineering architecture using Python, Databricks, Snowflake and dbt.

---

# Overall Architecture

```text
NPPES Provider Registry API
            │
            ▼
Python Extraction Layer
            │
            ▼
JSIB Response Processing
            │
            ▼
JSIB Payload Transformation
            │
            ▼
Processed JSON
            │
            ▼
Databricks Bronze
            │
            ▼
Databricks Silver
            │
            ▼
Databricks Gold
            │
            ▼
Snowflake
            │
            ▼
dbt Models
            │
            ▼
Analytics / Downstream Systems
```

---

# Repository Creation

A GitHub repository was created for the project.

The repository is used to store the complete source code, project documentation, Databricks notebooks, Snowflake scripts and future enhancements.

Git was initialized locally and the project was pushed successfully to GitHub.

---

# Project Folder Structure

The following enterprise folder structure was created.

```text
Healthcare_MDM/

│
├── config/
│
├── data/
│   ├── raw/
│   └── processed/
│
├── databricks/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── docs/
│
├── snowflake/
│   ├── dev/
│   ├── test/
│   └── prod/
│
├── src/
│   ├── common/
│   ├── extract/
│   ├── transform/
│   ├── deduplicate/
│   └── load/
│
├── .env
├── .gitignore
├── requirements.txt
└── README.md
```

---

# Python Virtual Environment

A dedicated virtual environment was created for this project.

All project dependencies are installed inside the virtual environment.

Installed packages include:

* requests
* python-dotenv
* snowflake-connector-python

Additional packages will be installed as the project grows.

---

# Environment Configuration

A .env file was created.

The environment file stores:

* Environment Name
* NPPES API URL
* API Version
* Snowflake Credentials
* Snowflake Database
* Snowflake Warehouse
* Bronze Schema
* Silver Schema
* Gold Schema
* Databricks Catalog
* Logging Configuration

This allows the project to keep sensitive information outside the source code.

---

# Snowflake Setup

The Snowflake environment was prepared.

Created objects include:

Warehouse

HEALTHCARE_WH

Database

HEALTHCARE_DEV

Schemas

BRONZE

SILVER

GOLD

These schemas will store different stages of provider data during the project.

---

# Databricks Setup

A Databricks Free Edition workspace was configured.

Created objects include:

Catalog

Healthcare_MDM

Schemas

bronze

silver

gold

Workspace Folder

Healthcare_MDM

Notebook execution was verified successfully.

---

# Common Utility Files

The following reusable modules were created.

logger.py

Used for application logging.

constants.py

Stores reusable constants used throughout the project.

helpers.py

Stores helper functions that can be reused by different modules.

---

# Documentation

Project documentation was created.

Current documentation includes:

01_source_analysis.md

This document explains:

* Source System
* Business Requirement
* Architecture
* Why NPPES was selected

---

# Source System Selection

After evaluating multiple healthcare APIs, the NPPES Provider Registry API was selected.

Reason:

* Public API
* Free to use
* Real healthcare provider data
* JSON response
* No API key required
* Suitable for enterprise MDM projects

NPPES replaces the source API that was represented by JSIB in the original enterprise project.

---

# Extraction Layer

The first Python extraction module was created.

File

src/extract/nppes_extract.py

Responsibilities

* Connect to the NPPES API
* Download provider records
* Save raw JSON response
* Call the JSIB response processor
* Call the JSIB payload transformation
* Save the processed payload

---

# Response Processing Layer

File

src/transform/process_jsib_response.py

Responsibilities

* Read raw NPPES JSON
* Extract required provider fields
* Flatten nested JSON
* Create a standardized provider object

This module performs only response transformation.

It does not perform:

* Deduplication
* Data Quality Rules
* Business Rules
* Database Loading

---

# JSIB Payload Transformation

File

src/transform/transform_to_jsib.py

Responsibilities

Convert the standardized provider object into the JSIB payload format.

The module creates:

* codBases
* fields
* matching methods
* fuzzy precision

This keeps the project similar to the original enterprise implementation.

---

# Data Storage

Two folders were created.

data/raw

Stores the original NPPES API response.

Example

nppes_raw_response.json

This file is never modified.

It acts as the source of truth.

---

data/processed

Stores the transformed JSIB payload.

Example

processed_response.json

This file is generated after response transformation.

It is the file that will be loaded into Databricks Bronze.

---

# Current Workflow

```text
NPPES API
      │
      ▼
nppes_extract.py
      │
      ▼
Raw API Response
      │
      ▼
data/raw/nppes_raw_response.json
      │
      ▼
process_jsib_response.py
      │
      ▼
Standard Provider Object
      │
      ▼
transform_to_jsib.py
      │
      ▼
JSIB Payload
      │
      ▼
data/processed/processed_response.json
```

---

# Current Status

Completed

* GitHub Repository
* Project Structure
* Virtual Environment
* Requirements Installation
* .gitignore
* .env Configuration
* Snowflake Setup
* Databricks Setup
* Source Analysis
* Utility Modules
* NPPES API Integration
* Raw JSON Extraction
* Response Processing
* JSIB Payload Transformation
* Processed JSON Generation

The Python extraction pipeline has been executed successfully and generated both the raw response and the processed response.

---

# Next Phase

The next implementation phase is the Bronze Layer.

The Bronze layer will read the processed JSON file and load it into a Delta table inside Databricks.

Bronze Table

provider_raw

Columns

* PROVIDER_JSON
* SOURCE_SYSTEM
* LOAD_TIMESTAMP

After the Bronze layer is completed, the project will continue with:

* Silver Layer
* Gold Layer
* Snowflake Loading
* dbt Models
* Analytics Layer
* Downstream Data Consumption

---

# Project Status

Current Progress

Phase 1

Completed

The complete extraction layer has been built successfully.

The project is now ready to begin the Databricks Bronze implementation.










============================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================

# Module-2 Progress Documentation

# Module Name

Databricks Bronze Layer

---

# Objective

The objective of this module is to ingest the processed provider payload generated by the Python extraction layer into the Databricks Bronze layer.

The Bronze layer preserves the processed provider payload exactly as it is received without performing any business transformations.

The Bronze layer also enriches every record with technical metadata required for auditing, lineage tracking, troubleshooting and future incremental processing.

---

# Input

The input file for this module is:

```
processed_response.json
```

The file is generated by the Python Extraction Layer after completing the following activities:

* Extract provider data from the NPPES API.
* Process the API response.
* Standardize the provider information.
* Transform the provider record into the JSIB payload format.
* Generate the processed provider JSON file.

---

# Databricks Storage

Instead of storing the processed file inside the Databricks Workspace, a Managed Volume was created.

Volume Name

```
Healthcare_MDM.bronze.bronze_files
```

Folder Structure

```
bronze_files

│

├── raw

├── processed

│      └── processed_response.json

└── checkpoint
```

The processed JSON file is uploaded into the **processed** folder inside the Bronze Volume.

This architecture prepares the project for future Auto Loader implementation.

---

# Activities Performed

## Step-1

Created a Spark Session.

---

## Step-2

Read the processed JSON file from the Bronze Volume.

```
/Volumes/Healthcare_MDM/bronze/bronze_files/processed/processed_response.json
```

---

## Step-3

Validated the JSON schema.

The processed provider payload contains:

* codBases
* entityType
* fields
* resultSize

Each provider record now contains approximately twenty-five JSIB mapped business fields.

---

## Step-4

Created the Bronze DataFrame.

The complete provider payload was converted into a single JSON document.

No business attributes were modified.

---

## Step-5

Added technical metadata columns.

The following metadata columns were created.

* PROVIDER_JSON
* SOURCE_SYSTEM
* LOAD_TIMESTAMP
* LOAD_DATE
* FILE_NAME
* BATCH_ID

---

# Metadata Description

## PROVIDER_JSON

Stores the complete processed provider payload as a JSON string.

Purpose

Preserve the complete business payload without losing any information.

---

## SOURCE_SYSTEM

Value

```
NPPES
```

Purpose

Identify the source application responsible for producing the provider record.

---

## LOAD_TIMESTAMP

Stores the exact timestamp when the provider record entered the Bronze layer.

Purpose

Supports auditing, lineage tracking and troubleshooting.

---

## LOAD_DATE

Stores only the ingestion date.

Purpose

Supports partitioning and incremental processing.

---

## FILE_NAME

Value

```
processed_response.json
```

Purpose

Identify the source file responsible for loading the provider record.

---

## BATCH_ID

Example

```
BATCH_001
```

Purpose

Track every ingestion execution.

This column will later be generated dynamically during automated pipeline execution.

---

# Bronze Table

Table Name

```
Healthcare_MDM.bronze.provider_raw
```

Storage Format

```
Delta Lake
```

Write Mode

```
Overwrite
```

Current Status

Development Phase

The table is recreated during development whenever the schema changes.

---

# Bronze Layer Responsibilities

The Bronze layer performs only technical ingestion.

It does not perform any business transformations.

The Bronze layer does NOT perform:

* Data Cleansing
* Data Standardization
* Deduplication
* Record Matching
* Survivorship Rules
* Business Rule Validation
* Golden Record Creation

These activities will be implemented in later modules.

---

# Current End-to-End Workflow

```
NPPES API
      │
      ▼
Python Extraction
      │
      ▼
process_jsib_response.py
      │
      ▼
transform_to_jsib.py
      │
      ▼
processed_response.json
      │
      ▼
Databricks Managed Volume
      │
      ▼
Healthcare_MDM.bronze.provider_raw
```

---

# Future Architecture

The Managed Volume has been introduced to simplify future production deployment.

The planned architecture will be:

```
NPPES API
      │
      ▼
Python Extraction
      │
      ▼
AWS S3
      │
      ▼
Databricks Auto Loader
      │
      ▼
Bronze Volume
      │
      ▼
Bronze Delta Table
```

Only the ingestion mechanism will change.

The Bronze processing notebook will remain almost unchanged.

---

# Module Status

Completed

The Bronze ingestion layer has been implemented successfully.

The processed provider payload is now stored inside the Databricks Bronze Delta table together with ingestion metadata.

The project is now ready to begin the Silver Layer, where the provider payload will be parsed, flattened, standardized and cleaned before preparing the business-ready dataset.



============================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================


