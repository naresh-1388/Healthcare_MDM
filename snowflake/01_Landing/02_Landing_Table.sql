/******************************************************************************
Project         : Healthcare Master Data Management (Healthcare_MDM)

File Name       : 02_Landing_Table.sql

Author          : Naresh

Layer           : Landing Layer

Object Type     : Table

Object Name     : PROVIDER_RAW

Purpose
-------
The PROVIDER_RAW table is the Landing Layer of the Healthcare MDM
pipeline.

This table stores raw provider data loaded automatically from AWS S3
through Snowpipe.

No transformations, validations or business rules are applied in this
layer. Data is stored exactly as received from the source system.

This table acts as the source for Snowflake Streams, which capture
incremental changes for downstream Tasks.

Pipeline Flow
-------------
AWS S3
   ↓
External Stage
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

Business Benefits
-----------------
• Maintains original source data.
• Supports auditing and troubleshooting.
• Allows data reprocessing if required.
• Enables Change Data Capture (CDC) using Streams.
• Serves as the foundation for downstream transformations.

******************************************************************************/

-- Switch to ACCOUNTADMIN role
-- ACCOUNTADMIN has permission to create and manage database objects.

USE ROLE ACCOUNTADMIN;

-- Select Warehouse
-- Warehouse is required for DDL and DML execution.

USE WAREHOUSE ORIEO_WH;

-- Select Database

USE DATABASE ORIEO_DB;

-- Select Landing Schema

USE SCHEMA LANDING_SC;

--------------------------------------------------------------------------------
-- Create Landing Table
--------------------------------------------------------------------------------

CREATE OR REPLACE TABLE PROVIDER_RAW
(
    ProviderID               STRING,             -- Unique Provider Identifier

    FullName                 STRING,             -- Provider Full Name

    Credential               STRING,             -- Medical Degree or Certification

    Gender                   STRING,             -- Provider Gender

    Status                   STRING,             -- Provider Status

    AddressLine1             STRING,             -- Primary Address

    City                     STRING,             -- City

    State                    STRING,             -- State

    PostalCode               STRING,             -- Postal Code

    CountryCode              STRING,             -- ISO Country Code

    PhoneNumber              STRING,             -- Contact Number

    TaxonomyCode             STRING,             -- Healthcare Taxonomy Code

    LicenseNumber            STRING,             -- Medical License Number

    Identifier               STRING,             -- External Identifier

    BUSINESS_RULE_STATUS     STRING,             -- Business Rule Validation Result

    DATA_QUALITY_WARNING     STRING,             -- Data Quality Warning

    SOURCE_SYSTEM            STRING,             -- Source Application

    PAYLOAD_FORMAT           STRING,             -- Incoming Payload Format

    TARGET_SYSTEM            STRING,             -- Destination System

    LOAD_TIMESTAMP           TIMESTAMP_NTZ,      -- Databricks Load Timestamp

    LOAD_DATE                DATE,               -- Data Load Date

    FILE_NAME                STRING,             -- Source File Name

    BATCH_ID                 STRING              -- ETL Batch Identifier
);

--------------------------------------------------------------------------------
-- Verify Table Creation
--------------------------------------------------------------------------------

SHOW TABLES LIKE 'PROVIDER_RAW';

--------------------------------------------------------------------------------
-- Describe Table Structure
--------------------------------------------------------------------------------

DESC TABLE PROVIDER_RAW;

--------------------------------------------------------------------------------
-- Display Column Information
--------------------------------------------------------------------------------

SHOW COLUMNS IN TABLE PROVIDER_RAW;

--------------------------------------------------------------------------------
-- Read Complete Table
--------------------------------------------------------------------------------

SELECT *
FROM PROVIDER_RAW;

--------------------------------------------------------------------------------
-- Read First 10 Records
--------------------------------------------------------------------------------

SELECT *
FROM PROVIDER_RAW
LIMIT 10;

--------------------------------------------------------------------------------
-- Count Total Records
--------------------------------------------------------------------------------

SELECT COUNT(*) AS TOTAL_RECORDS
FROM PROVIDER_RAW;

--------------------------------------------------------------------------------
-- View Latest Loaded Records
--------------------------------------------------------------------------------

SELECT *
FROM PROVIDER_RAW
ORDER BY LOAD_TIMESTAMP DESC;

--------------------------------------------------------------------------------
-- View Latest Batch Loaded
--------------------------------------------------------------------------------

SELECT DISTINCT
       BATCH_ID,
       FILE_NAME,
       LOAD_DATE
FROM PROVIDER_RAW
ORDER BY LOAD_DATE DESC;

--------------------------------------------------------------------------------
-- View Provider Count by Source System
--------------------------------------------------------------------------------

SELECT
    SOURCE_SYSTEM,
    COUNT(*) AS TOTAL_PROVIDERS
FROM PROVIDER_RAW
GROUP BY SOURCE_SYSTEM;

--------------------------------------------------------------------------------
-- View Provider Count by Country
--------------------------------------------------------------------------------

SELECT
    CountryCode,
    COUNT(*) AS TOTAL_PROVIDERS
FROM PROVIDER_RAW
GROUP BY CountryCode
ORDER BY TOTAL_PROVIDERS DESC;

--------------------------------------------------------------------------------
-- View Provider Count by State
--------------------------------------------------------------------------------

SELECT
    State,
    COUNT(*) AS TOTAL_PROVIDERS
FROM PROVIDER_RAW
GROUP BY State
ORDER BY TOTAL_PROVIDERS DESC;

--------------------------------------------------------------------------------
-- Find Duplicate Providers
--------------------------------------------------------------------------------

SELECT
    ProviderID,
    COUNT(*) AS DUPLICATE_COUNT
FROM PROVIDER_RAW
GROUP BY ProviderID
HAVING COUNT(*) > 1;

--------------------------------------------------------------------------------
-- View Data Quality Issues
--------------------------------------------------------------------------------

SELECT *
FROM PROVIDER_RAW
WHERE DATA_QUALITY_WARNING IS NOT NULL;

--------------------------------------------------------------------------------
-- View Failed Business Rules
--------------------------------------------------------------------------------

SELECT *
FROM PROVIDER_RAW
WHERE BUSINESS_RULE_STATUS <> 'PASS';

--------------------------------------------------------------------------------
-- Remove All Records
--
-- TRUNCATE removes only table data.
-- Table structure remains unchanged.
--------------------------------------------------------------------------------

TRUNCATE TABLE PROVIDER_RAW;

--------------------------------------------------------------------------------
-- Permanently Remove Table
--
-- Use only during development.
--------------------------------------------------------------------------------

DROP TABLE IF EXISTS PROVIDER_RAW;