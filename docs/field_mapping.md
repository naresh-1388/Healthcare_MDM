# Field Mapping

## Overview
This document describes how healthcare provider data flows through every layer of the Healthcare_MDM pipeline, from the NPPES Registry API to the final Power BI dashboard.

---

## End-to-End Column Lineage

```
NPPES API
    ↓
Python Standardization
    ↓
JISB Payload
    ↓
Bronze Delta
    ↓
Silver Delta
    ↓
Gold Delta
    ↓
S3 Gold Parquet
    ↓
Snowflake Landing
    ↓
Snowflake Gold
    ↓
dbt Models
    ↓
Power BI Dashboard
```

---

## Source → Standardized Mapping

| Source Field | Standardized Field |
|--------------|-------------------|
| number | ProviderID |
| basic.first_name | FirstName |
| basic.middle_name | MiddleName |
| basic.last_name | LastName |
| Full Name | FullName |
| basic.gender | Gender |
| basic.status | Status |
| basic.credential | Credential |
| basic.enumeration_date | EnumerationDate |
| basic.last_updated | LastUpdated |
| addresses.address_1 | AddressLine1 |
| addresses.city | City |
| addresses.state | State |
| addresses.postal_code | PostalCode |
| addresses.country_code | CountryCode |
| addresses.country_name | CountryName |
| addresses.telephone_number | PhoneNumber |
| addresses.fax_number | FaxNumber |
| taxonomies.code | TaxonomyCode |
| taxonomies.desc | TaxonomyDescription |
| taxonomies.license | LicenseNumber |
| taxonomies.state | LicenseState |
| identifiers.identifier | Identifier |
| identifiers.issuer | IdentifierIssuer |
| identifiers.code | IdentifierType |

---

## Standardized → JISB Mapping

| Standardized | JISB Field | Match Method | Precision |
|--------------|------------|--------------|-----------|
| ProviderID | ProviderID | EXACT | 100 |
| FullName | FullName | FUZZY | 95 |
| Credential | Credential | EXACT | 100 |
| Gender | Gender | EXACT | 100 |
| Status | Status | EXACT | 100 |
| AddressLine1 | AddressLine1 | FUZZY | 90 |
| City | City | FUZZY | 90 |
| State | State | EXACT | 100 |
| PostalCode | PostalCode | EXACT | 100 |
| CountryCode | CountryCode | EXACT | 100 |
| PhoneNumber | PhoneNumber | EXACT | 100 |
| TaxonomyCode | TaxonomyCode | EXACT | 100 |
| TaxonomyDescription | TaxonomyDescription | FUZZY | 90 |
| LicenseNumber | LicenseNumber | EXACT | 100 |
| Identifier | Identifier | EXACT | 100 |

---

## Bronze Schema

| Column |
|--------|
| PROVIDER_JSON |
| SOURCE_SYSTEM |
| PAYLOAD_FORMAT |
| TARGET_SYSTEM |
| LOAD_TIMESTAMP |
| LOAD_DATE |
| FILE_NAME |
| BATCH_ID |

---

## Silver Schema

### Business Columns

- ProviderID
- FirstName
- MiddleName
- LastName
- FullName
- Credential
- Gender
- Status
- EnumerationDate
- LastUpdated
- AddressLine1
- City
- State
- PostalCode
- CountryCode
- CountryName
- PhoneNumber
- FaxNumber
- TaxonomyCode
- TaxonomyDescription
- LicenseNumber
- LicenseState
- Identifier
- IdentifierIssuer
- IdentifierType

### Metadata

- SOURCE_SYSTEM
- PAYLOAD_FORMAT
- TARGET_SYSTEM
- LOAD_TIMESTAMP
- LOAD_DATE
- FILE_NAME
- BATCH_ID

---

## Gold Schema

### Business Columns

- ProviderID
- FullName
- Credential
- Gender
- Status
- AddressLine1
- City
- State
- PostalCode
- CountryCode
- PhoneNumber
- TaxonomyCode
- LicenseNumber
- Identifier

### MDM Columns

- BUSINESS_RULE_STATUS
- DATA_QUALITY_WARNING

### Audit Columns

- SOURCE_SYSTEM
- PAYLOAD_FORMAT
- TARGET_SYSTEM
- LOAD_TIMESTAMP
- LOAD_DATE
- FILE_NAME
- BATCH_ID

---

## Snowflake Mapping

| Databricks Gold | Snowflake Landing | Snowflake Gold |
|-----------------|-------------------|----------------|
| ProviderID | ProviderID | ProviderID |
| FullName | FullName | FullName |
| Credential | Credential | Credential |
| Gender | Gender | Gender |
| Status | Status | Status |
| AddressLine1 | AddressLine1 | AddressLine1 |
| City | City | City |
| State | State | State |
| PostalCode | PostalCode | PostalCode |
| CountryCode | CountryCode | CountryCode |
| PhoneNumber | PhoneNumber | PhoneNumber |
| TaxonomyCode | TaxonomyCode | TaxonomyCode |
| LicenseNumber | LicenseNumber | LicenseNumber |
| Identifier | Identifier | Identifier |
| BUSINESS_RULE_STATUS | BUSINESS_RULE_STATUS | BUSINESS_RULE_STATUS |
| DATA_QUALITY_WARNING | DATA_QUALITY_WARNING | DATA_QUALITY_WARNING |
| SOURCE_SYSTEM | SOURCE_SYSTEM | SOURCE_SYSTEM |
| PAYLOAD_FORMAT | PAYLOAD_FORMAT | PAYLOAD_FORMAT |
| TARGET_SYSTEM | TARGET_SYSTEM | TARGET_SYSTEM |
| LOAD_TIMESTAMP | LOAD_TIMESTAMP | LOAD_TIMESTAMP |
| LOAD_DATE | LOAD_DATE | LOAD_DATE |
| FILE_NAME | FILE_NAME | FILE_NAME |
| BATCH_ID | BATCH_ID | BATCH_ID |

---

## dbt Models

```
PROVIDER_GOLD
      │
      ▼
STG_PROVIDER
      │
      ▼
DIM_PROVIDER
      │
      ├── MART_PROVIDER_STATE
      ├── MART_PROVIDER_SUMMARY
      ├── MART_PROVIDER_GENDER
      ├── MART_PROVIDER_TAXONOMY
      ├── MART_PROVIDER_BUSINESS_RULES
      └── MART_PROVIDER_DATA_QUALITY
```

---

## Power BI Tables

- DIM_PROVIDER
- MART_PROVIDER_STATE
- MART_PROVIDER_SUMMARY
- MART_PROVIDER_GENDER
- MART_PROVIDER_TAXONOMY
- MART_PROVIDER_BUSINESS_RULES
- MART_PROVIDER_DATA_QUALITY

---

## Complete Data Flow

```
NPPES API
      ↓
Python Extraction
      ↓
Standardization
      ↓
JISB Payload
      ↓
AWS S3 (Processed)
      ↓
Bronze
      ↓
Silver
      ↓
Gold
      ↓
AWS S3 (Gold)
      ↓
Snowpipe
      ↓
Provider_RAW
      ↓
Provider_RAW_STREAM
      ↓
Provider_GOLD
      ↓
dbt
      ↓
Analytics Marts
      ↓
Power BI
```

---

## Summary

The Healthcare_MDM pipeline preserves complete field lineage across every processing layer. Business attributes, metadata, audit columns, and MDM validation fields remain traceable from the original NPPES API response through Python processing, Databricks Medallion Architecture, Snowflake, dbt semantic models, and finally the Power BI dashboard.