{{ config(materialized='table') }}

SELECT

ProviderID,
FullName,
Credential,
Gender,
Status,
AddressLine1,
City,
State,
PostalCode,
CountryCode,
PhoneNumber,
TaxonomyCode,
LicenseNumber,
Identifier,
BUSINESS_RULE_STATUS,
DATA_QUALITY_WARNING,
SOURCE_SYSTEM,
PAYLOAD_FORMAT,
TARGET_SYSTEM,
LOAD_TIMESTAMP,
LOAD_DATE,
FILE_NAME,
BATCH_ID

FROM {{ source('gold','provider_gold') }}



-- dbt run --select dim_provider --full-refresh