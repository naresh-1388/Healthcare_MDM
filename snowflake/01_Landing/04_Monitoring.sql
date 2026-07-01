/******************************************************************************
Project         : Healthcare Master Data Management (Healthcare_MDM)

File Name       : 04_Monitoring.sql

Author          : Naresh

Layer           : Landing Layer

Purpose
-------
This script contains monitoring and validation queries for the
Landing Layer.

These queries help monitor Snowpipe, Stage and Landing table.

No data is modified by this script.

It is mainly used by Data Engineers to validate whether files
have been loaded successfully.

******************************************************************************/

USE ROLE ACCOUNTADMIN;

USE WAREHOUSE ORIEO_WH;

USE DATABASE ORIEO_DB;

USE SCHEMA LANDING_SC;

--------------------------------------------------------------------------------
-- Verify files available inside External Stage
--------------------------------------------------------------------------------

LIST @ORIEO_STAGE;

--------------------------------------------------------------------------------
-- Display all Snowpipes
--------------------------------------------------------------------------------

SHOW PIPES;

--------------------------------------------------------------------------------
-- Describe Snowpipe
--------------------------------------------------------------------------------

DESC PIPE ORIEO_PIPE;

--------------------------------------------------------------------------------
-- Check current Snowpipe Status
--------------------------------------------------------------------------------

SELECT SYSTEM$PIPE_STATUS('ORIEO_PIPE');

--------------------------------------------------------------------------------
-- Display Landing Table Structure
--------------------------------------------------------------------------------

DESC TABLE PROVIDER_RAW;

--------------------------------------------------------------------------------
-- Display Landing Table Columns
--------------------------------------------------------------------------------

SHOW COLUMNS IN TABLE PROVIDER_RAW;

--------------------------------------------------------------------------------
-- Count Total Records
--------------------------------------------------------------------------------

SELECT COUNT(*) AS TOTAL_RECORDS
FROM PROVIDER_RAW;

--------------------------------------------------------------------------------
-- View Sample Records
--------------------------------------------------------------------------------

SELECT *
FROM PROVIDER_RAW
LIMIT 10;

--------------------------------------------------------------------------------
-- View Latest Records
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
-- Provider Count by Source System
--------------------------------------------------------------------------------

SELECT
SOURCE_SYSTEM,
COUNT(*) TOTAL_RECORDS
FROM PROVIDER_RAW
GROUP BY SOURCE_SYSTEM;

--------------------------------------------------------------------------------
-- Provider Count by Country
--------------------------------------------------------------------------------

SELECT
CountryCode,
COUNT(*) TOTAL_RECORDS
FROM PROVIDER_RAW
GROUP BY CountryCode
ORDER BY TOTAL_RECORDS DESC;

--------------------------------------------------------------------------------
-- Provider Count by State
--------------------------------------------------------------------------------

SELECT
State,
COUNT(*) TOTAL_RECORDS
FROM PROVIDER_RAW
GROUP BY State
ORDER BY TOTAL_RECORDS DESC;

--------------------------------------------------------------------------------
-- Duplicate Provider Check
--------------------------------------------------------------------------------

SELECT
ProviderID,
COUNT(*) DUPLICATE_COUNT
FROM PROVIDER_RAW
GROUP BY ProviderID
HAVING COUNT(*) > 1;

--------------------------------------------------------------------------------
-- Business Rule Validation Report
--------------------------------------------------------------------------------

SELECT
BUSINESS_RULE_STATUS,
COUNT(*) TOTAL_RECORDS
FROM PROVIDER_RAW
GROUP BY BUSINESS_RULE_STATUS;

--------------------------------------------------------------------------------
-- Data Quality Warning Report
--------------------------------------------------------------------------------

SELECT
DATA_QUALITY_WARNING,
COUNT(*) TOTAL_RECORDS
FROM PROVIDER_RAW
GROUP BY DATA_QUALITY_WARNING;

--------------------------------------------------------------------------------
-- Snowpipe Copy History
--
-- Shows every file loaded into PROVIDER_RAW.
--------------------------------------------------------------------------------

SELECT *
FROM TABLE
(
INFORMATION_SCHEMA.COPY_HISTORY
(
TABLE_NAME=>'PROVIDER_RAW',
START_TIME=>DATEADD('DAY',-7,CURRENT_TIMESTAMP())
)
);

--------------------------------------------------------------------------------
-- Recently Executed COPY Commands
--------------------------------------------------------------------------------

SELECT *
FROM TABLE
(
INFORMATION_SCHEMA.LOAD_HISTORY
(
START_TIME=>DATEADD('DAY',-7,CURRENT_TIMESTAMP())
)
);

--------------------------------------------------------------------------------
-- Recently Executed Queries
--------------------------------------------------------------------------------

SELECT
QUERY_TEXT,
START_TIME,
EXECUTION_STATUS
FROM TABLE
(
INFORMATION_SCHEMA.QUERY_HISTORY()
)
ORDER BY START_TIME DESC
LIMIT 20;

--------------------------------------------------------------------------------
-- Warehouse Information
--------------------------------------------------------------------------------

SHOW WAREHOUSES;

--------------------------------------------------------------------------------
-- Stage Information
--------------------------------------------------------------------------------

DESC STAGE ORIEO_STAGE;