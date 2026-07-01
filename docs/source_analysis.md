# Source System Analysis

## Project Objective
The objective of this project is to build a Healthcare Master Data Management (MDM) pipeline that extracts healthcare provider data from the NPPES Registry API, standardizes and validates the records, generates trusted Golden Records, and makes them available for analytics through Snowflake, dbt, and Power BI.

---

## Source System

| Property | Value |
|----------|-------|
| Source | NPPES Registry API |
| Organization | Centers for Medicare & Medicaid Services (CMS) |
| Data Format | JSON |
| API Type | REST |
| Authentication | Public API (No Authentication Required) |

---

## API Endpoint

```text
https://npiregistry.cms.hhs.gov/api/
```

### Request Parameters

| Parameter | Description |
|-----------|-------------|
| version | API Version |
| city | Provider City |
| state | Provider State |
| enumeration_type | NPI-1 |
| limit | Number of Records |
| skip | Pagination Offset |

---

## Data Extraction Process

```
NPPES API
      │
      ▼
Python Request
      │
      ▼
Raw JSON
      │
      ▼
Standardization
      │
      ▼
JISB Payload
      │
      ▼
Processed JSON
```

---

## Source Data Characteristics

- Nested JSON Structure
- Multiple Addresses
- Multiple Taxonomies
- Multiple Identifiers
- Optional Attributes
- Missing Values
- Duplicate Providers Across Systems

---

## Mandatory Business Fields

- ProviderID
- FullName
- Status
- TaxonomyCode
- CountryCode

---

## Optional Fields

- PhoneNumber
- FaxNumber
- Identifier
- LicenseNumber
- Credential
- PostalCode

---

## Data Challenges

- Nested JSON Arrays
- Null Values
- Duplicate Provider Records
- Inconsistent Name Formats
- Multiple Addresses
- Multiple Taxonomies
- Missing Contact Information

---

## Business Rules

| Rule ID | Description |
|----------|-------------|
| BR-001 | ProviderID must not be NULL |
| BR-002 | Provider Status must be Active |
| BR-003 | TaxonomyCode must not be NULL |
| BR-004 | CountryCode must be US |

---

## Data Quality Rules

| Rule ID | Description |
|----------|-------------|
| DQ-001 | PhoneNumber Missing |
| DQ-002 | LicenseNumber Missing |
| DQ-003 | Identifier Missing |

---

## Source to Processing Flow

```
NPPES API
      │
      ▼
Python Extraction
      │
      ▼
Raw JSON
      │
      ▼
Processed JSON
      │
      ▼
AWS S3
      │
      ▼
Databricks Bronze
```

---

## Output of Source Layer

The source layer produces standardized JISB payloads that are stored in AWS S3 Processed storage. These files become the input for the Databricks Bronze layer and initiate the downstream Medallion Architecture pipeline.