# Source Analysis

## Project Name

Healthcare_MDM

---

# Source System

Source Name:
NPPES Provider Registry API

Official Website:
https://npiregistry.cms.hhs.gov/

API Endpoint:
https://npiregistry.cms.hhs.gov/api/

Purpose:
The NPPES API provides healthcare provider information such as doctors, physicians, hospitals, and other healthcare professionals registered in the United States.

For this project, the NPPES API acts as our source system (similar to JSIB in the original project).

---

# Why We Selected This API

We selected the NPPES API because:

- It is free to use.
- No API key is required.
- It provides real healthcare provider information.
- It returns data in JSON format.
- It is suitable for Data Engineering projects.
- It supports provider search.
- It contains real provider identifiers.

This makes it a good source system for building a Healthcare Master Data Management (MDM) project.

---

# Business Requirement

The client wants to build a centralized Healthcare Provider Master.

The system should:

- Extract provider data from NPPES.
- Store the raw data.
- Clean and standardize the data.
- Remove duplicate providers.
- Create a Golden Provider Record.
- Load the final provider data into Snowflake.
- Make the data available for downstream systems.

---

# Source Type

REST API

Response Format:
JSON

Authentication:
No authentication required.

API Key:
Not required.

---

# Expected Search Parameters

The API supports searching providers using different filters such as:

- NPI Number
- First Name
- Last Name
- Organization Name
- City
- State
- Postal Code
- Taxonomy
- Enumeration Type

These filters help retrieve specific healthcare providers.

---

# Expected Response

The API returns healthcare provider information.

Common fields include:

- NPI Number
- Provider Name
- Organization Name
- Address
- City
- State
- Postal Code
- Country
- Taxonomy
- Provider Type
- Enumeration Date
- Last Updated Date
- Status

The exact response depends on the search request.

---

# Primary Business Key

NPI Number

Reason:

The NPI (National Provider Identifier) is unique for every healthcare provider.

This will be used as the primary identifier throughout the project.

---

# Project Architecture

NPPES API
      |
      V
Python Extraction
      |
      V
Databricks Bronze
      |
      V
Databricks Silver
      |
      V
Databricks Gold
      |
      V
Snowflake
      |
      V
Downstream Systems

---

# Bronze Layer

Purpose:

Store the raw API response without making any changes.

No transformation.

No deduplication.

No business rules.

---

# Silver Layer

Purpose:

Clean and standardize the provider data.

Activities:

- Extract required fields.
- Standardize names.
- Standardize addresses.
- Handle missing values.
- Prepare data for business processing.

---

# Gold Layer

Purpose:

Create the final Provider Master.

Activities:

- Deduplicate providers.
- Create Golden Records.
- Apply business rules.
- Prepare final reporting data.

---

# Data Engineering Responsibilities

- API Integration
- Data Extraction
- Data Validation
- Data Cleaning
- Standardization
- Deduplication
- Master Data Creation
- Data Loading
- Data Quality Checks

---

# Future Enhancements

Later phases of the project will include:

- Incremental Loads
- Change Data Capture (CDC)
- Databricks Workflows
- dbt Models
- Snowflake Tasks
- Monitoring
- Logging
- Error Handling
- Scheduling