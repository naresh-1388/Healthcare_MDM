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

The Bronze layer enriches every provider record with technical metadata required for auditing, lineage tracking, troubleshooting and future incremental processing.

---

# Input

The input file for this module is:

```
processed_response.json
```

The file is generated by the Python Extraction Layer after completing the following activities:

* Extract provider data from the NPPES API.
* Process the NPPES response.
* Standardize the provider information.
* Transform the provider record into the JSIB payload format.
* Generate the processed provider JSON file.

---

# Databricks Storage

A Managed Volume was created to store the processed provider file.

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

The processed provider file is uploaded into the **processed** folder.

This architecture prepares the project for future Auto Loader implementation.

---

# Activities Performed

## Step-1

Created a Spark Session.

---

## Step-2

Read the processed provider JSON file from the Managed Volume.

```
/Volumes/Healthcare_MDM/bronze/bronze_files/processed/processed_response.json
```

---

## Step-3

Validated the provider JSON schema.

The processed payload contains the following top-level attributes.

* resultSize
* entityType
* codBases
* fields

Each provider record contains approximately twenty-five JSIB mapped business attributes.

---

## Step-4

Created the Bronze DataFrame.

The complete provider payload was converted into a single JSON document without modifying any business information.

---

## Step-5

Added technical metadata columns.

The following metadata columns were created.

* PROVIDER_JSON
* SOURCE_SYSTEM
* PAYLOAD_FORMAT
* TARGET_SYSTEM
* LOAD_TIMESTAMP
* LOAD_DATE
* FILE_NAME
* BATCH_ID

---

# Metadata Description

## PROVIDER_JSON

Stores the complete processed provider payload as a JSON string.

Purpose

Preserve the complete provider payload exactly as received.

---

## SOURCE_SYSTEM

Value

```
NPPES
```

Purpose

Identifies the original source system from which the provider data was extracted.

---

## PAYLOAD_FORMAT

Value

```
JSIB
```

Purpose

Identifies the canonical payload structure generated by the transformation layer before loading into Databricks.

---

## TARGET_SYSTEM

Value

```
ORIEO
```

Purpose

Identifies the downstream application that will consume the standardized provider records after MDM processing.

---

## LOAD_TIMESTAMP

Stores the exact timestamp when the provider record entered the Bronze layer.

Purpose

Supports auditing, lineage tracking and troubleshooting.

---

## LOAD_DATE

Stores only the ingestion date.

Purpose

Supports partitioning and future incremental processing.

---

## FILE_NAME

Value

```
processed_response.json
```

Purpose

Identifies the source file used for loading the provider records.

---

## BATCH_ID

Example

```
BATCH_001
```

Purpose

Tracks every ingestion execution.

This value is currently hardcoded during development and will be generated dynamically after pipeline automation.

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

The Bronze table is recreated whenever the schema changes during development.

---

# Bronze Layer Responsibilities

The Bronze layer performs only technical ingestion.

No business transformations are performed in this layer.

The Bronze layer does NOT perform:

* Data Cleansing
* Data Standardization
* Deduplication
* Record Matching
* Survivorship Rules
* Business Rule Validation
* Golden Record Creation

These activities will be implemented in the Silver and Gold layers.

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
JSIB Payload
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

# Metadata Flow

```
SOURCE_SYSTEM  = NPPES

PAYLOAD_FORMAT = JSIB

TARGET_SYSTEM  = ORIEO
```

---

# Future Architecture

The Managed Volume prepares the project for production automation.

Future ingestion architecture will be:

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
Managed Volume
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

The processed provider payload is now stored inside the Databricks Bronze Delta table together with complete ingestion metadata.

The project is now ready to begin the Silver Layer, where the provider payload will be parsed, flattened, standardized and prepared for downstream MDM processing before delivery to the ORIEO system.




============================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================
# Module-3 Progress Documentation

# Module Name

Databricks Silver Layer

---

# Objective

The objective of this module is to transform the raw provider data stored in the Bronze layer into a standardized and business-friendly dataset.

The Silver layer parses the provider payload, flattens the nested JSON structure, standardizes provider attributes, applies data quality improvements and prepares the dataset for downstream MDM processing.

Unlike the Bronze layer, the Silver layer performs technical transformations while preserving all available provider information.

No business matching, survivorship or Golden Record creation is performed in this module.

---

# Input

The input for this module is the Bronze Delta table.

Table Name

```text
Healthcare_MDM.bronze.provider_raw
```

The Bronze table contains:

* Complete JSIB provider payload
* Source metadata
* Payload metadata
* Target system metadata
* Ingestion metadata

---

# Activities Performed

## Step-1

Read the Bronze Delta table.

The complete provider payload together with ingestion metadata was loaded into Spark.

---

## Step-2

Parsed the PROVIDER_JSON column.

The JSON payload stored as a string was converted into a structured Spark object using a predefined schema.

The parsed provider payload contains:

* resultSize
* entityType
* codBases
* fields

---

## Step-3

Flattened the provider payload.

The nested **fields** array was exploded.

Each provider attribute became an individual record.

Example

```text
ProviderID

FirstName

MiddleName

LastName

City

PhoneNumber

TaxonomyCode

LicenseNumber
```

---

## Step-4

Created the flattened provider dataset.

Each flattened record contains:

* FIELD_NAME
* FIELD_VALUE
* MATCH_METHOD
* FUZZY_PRECISION

The ingestion metadata from the Bronze layer was also preserved.

Metadata retained:

* SOURCE_SYSTEM
* PAYLOAD_FORMAT
* TARGET_SYSTEM
* LOAD_TIMESTAMP
* LOAD_DATE
* FILE_NAME
* BATCH_ID

---

## Step-5

Pivoted the flattened dataset.

The provider attributes were converted from multiple rows into a single standardized provider record.

Example

Before Pivot

```text
ProviderID

FirstName

MiddleName

LastName
```

Multiple rows

↓

After Pivot

One provider record containing all provider attributes as individual columns.

---

## Step-6

Applied business column ordering.

The standardized provider columns were arranged in the required enterprise order.

Example

* ProviderID
* FirstName
* MiddleName
* LastName
* FullName
* Credential
* Gender
* Status
* EnumerationDate
* LastUpdated
* AddressLine1
* City
* State
* PostalCode
* CountryCode
* CountryName
* PhoneNumber
* FaxNumber
* TaxonomyCode
* TaxonomyDescription
* LicenseNumber
* LicenseState
* Identifier
* IdentifierIssuer
* IdentifierType

Technical metadata columns were placed at the end.

---

## Step-7

Performed data standardization.

The following standardization rules were applied dynamically to all string columns.

* Removed leading spaces.
* Removed trailing spaces.
* Converted empty strings into NULL values.

This implementation automatically supports future schema changes without modifying the notebook.

---

## Step-8

Generated the Data Quality Report.

A NULL count report was generated for every column.

The report provides visibility into data completeness without rejecting provider records.

No business rules were applied because no FRD or BRD was available.

---

## Step-9

Stored the standardized provider dataset.

Table Name

```text
Healthcare_MDM.silver.provider_standardized
```

Storage Format

```text
Delta Lake
```

Write Mode

```text
Overwrite
```

The overwrite mode is currently used during the development phase while the schema is evolving.

---

# Metadata Preserved

The following ingestion metadata remains available in the Silver layer.

* SOURCE_SYSTEM
* PAYLOAD_FORMAT
* TARGET_SYSTEM
* LOAD_TIMESTAMP
* LOAD_DATE
* FILE_NAME
* BATCH_ID

Metadata Values

```text
SOURCE_SYSTEM = NPPES

PAYLOAD_FORMAT = JSIB

TARGET_SYSTEM = ORIEO
```

---

# Silver Layer Responsibilities

The Silver layer performs:

* JSON Parsing
* Data Flattening
* Provider Standardization
* Business Column Ordering
* String Standardization
* Empty String Normalization
* Data Quality Reporting

The Silver layer does NOT perform:

* Duplicate Removal
* Record Matching
* Survivorship Rules
* Golden Record Creation
* ORIEO Matching

These business operations will be implemented in the Gold layer.

---

# Current End-to-End Workflow

```text
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
Managed Volume
      │
      ▼
Bronze Delta Table
      │
      ▼
JSON Parsing
      │
      ▼
Flatten Provider Fields
      │
      ▼
Pivot Provider Attributes
      │
      ▼
Business Column Ordering
      │
      ▼
String Standardization
      │
      ▼
Data Quality Report
      │
      ▼
Healthcare_MDM.silver.provider_standardized
```

---

# Future Processing

The Silver table will serve as the input for the Gold layer.

The Gold layer will implement:

* Business Rules
* Duplicate Detection
* Provider Matching
* Survivorship Rules
* Golden Record Creation
* Final ORIEO Output

---

# Module Status

Completed

The Silver layer has been successfully implemented.

The provider data has been parsed, flattened, standardized, quality checked and stored as a structured Delta table.

The project is now ready to begin the Gold layer, where business rules and MDM logic will be implemented.


====================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================

