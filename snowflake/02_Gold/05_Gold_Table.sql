/******************************************************************************
Project         : Healthcare Master Data Management (Healthcare_MDM)

File Name       : 05_Gold_Table.sql

Author          : Naresh

Layer           : Gold Layer

Object Type     : Table

Object Name     : PROVIDER_GOLD

Purpose
-------
PROVIDER_GOLD is the curated table of the Healthcare MDM pipeline.

Unlike the Landing table, this table stores only cleaned,
validated and business-ready provider data.

This table is automatically populated by a Snowflake Task
reading incremental records from a Stream.

Business users, reporting tools and dbt models should always
read from this table instead of the Landing table.

Pipeline
--------
AWS S3
   ↓
Snowpipe
   ↓
Landing.PROVIDER_RAW
   ↓
Stream
   ↓
Task
   ↓
Gold.PROVIDER_GOLD

******************************************************************************/

USE ROLE ACCOUNTADMIN;

USE WAREHOUSE ORIEO_WH;

USE DATABASE ORIEO_DB;

USE SCHEMA GOLD_SC;

--------------------------------------------------------------------------------
-- Create Gold Table
--------------------------------------------------------------------------------

CREATE OR REPLACE TABLE PROVIDER_GOLD
(
    ProviderID               STRING,
    FullName                 STRING,
    Credential               STRING,
    Gender                   STRING,
    Status                   STRING,
    AddressLine1             STRING,
    City                     STRING,
    State                    STRING,
    PostalCode               STRING,
    CountryCode              STRING,
    PhoneNumber              STRING,
    TaxonomyCode             STRING,
    LicenseNumber            STRING,
    Identifier               STRING,
    BUSINESS_RULE_STATUS     STRING,
    DATA_QUALITY_WARNING     STRING,
    SOURCE_SYSTEM            STRING,
    PAYLOAD_FORMAT           STRING,
    TARGET_SYSTEM            STRING,
    LOAD_TIMESTAMP           TIMESTAMP_NTZ,
    LOAD_DATE                DATE,
    FILE_NAME                STRING,
    BATCH_ID                 STRING
);




=============================================================================================================================================================================================================================================================================================
 Phase 1 : One Time Initial load
-- i should use this for Historical Load (One Time)

INSERT INTO ORIEO_DB.GOLD_SC.PROVIDER_GOLD
SELECT *
FROM ORIEO_DB.LANDING_SC.PROVIDER_RAW;


        --   Historical Data
        --         │
        --         ▼
        --      Landing
        --         │
        --         ▼
        -- One Time Initial Load
        --         │
        --         ▼
        --       Gold

----------------------------------------------------------------------------
Phase 2:
=== INCREMENTAL ===

-- S3 -->> Snowpipe -->> Landing -->> Stream -->> Task -->> Gold

===========================================================================================================================================================================================================================================================================================


--------------------------------------------------------------------------------
-- Verify Table
--------------------------------------------------------------------------------

SHOW TABLES LIKE 'PROVIDER_GOLD';

DESC TABLE PROVIDER_GOLD;

SHOW COLUMNS IN TABLE PROVIDER_GOLD;

--------------------------------------------------------------------------------
-- Read Data
--------------------------------------------------------------------------------

SELECT *
FROM PROVIDER_GOLD;

SELECT *
FROM PROVIDER_GOLD
LIMIT 10;

SELECT COUNT(*) AS TOTAL_RECORDS
FROM PROVIDER_GOLD;

--------------------------------------------------------------------------------
-- Latest Batch
--------------------------------------------------------------------------------

SELECT DISTINCT
BATCH_ID,
FILE_NAME,
LOAD_DATE
FROM PROVIDER_GOLD
ORDER BY LOAD_DATE DESC;

--------------------------------------------------------------------------------
-- Duplicate Provider Check
--------------------------------------------------------------------------------

SELECT
ProviderID,
COUNT(*) TOTAL_RECORDS
FROM PROVIDER_GOLD
GROUP BY ProviderID
HAVING COUNT(*)>1;

--------------------------------------------------------------------------------
-- Remove Data Only
--------------------------------------------------------------------------------

TRUNCATE TABLE PROVIDER_GOLD;

--------------------------------------------------------------------------------
-- Remove Table
--------------------------------------------------------------------------------

