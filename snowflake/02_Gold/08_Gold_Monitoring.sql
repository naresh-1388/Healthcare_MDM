/******************************************************************************
Project         : Healthcare Master Data Management (Healthcare_MDM)

File Name       : 08_Gold_Monitoring.sql

Author          : Naresh

Layer           : Gold Layer

Purpose
-------
This script contains monitoring and validation queries for the Gold Layer.

It helps Data Engineers verify:

• Gold table data
• Stream status
• Task status
• Task execution history
• Duplicate records
• Latest loaded batches
• Business rule validation
• Data quality validation

This file does NOT modify data.
It is used only for monitoring and troubleshooting.

******************************************************************************/

USE ROLE ACCOUNTADMIN;

USE WAREHOUSE ORIEO_WH;

USE DATABASE ORIEO_DB;

USE SCHEMA GOLD_SC;

--------------------------------------------------------------------------------
-- Verify Gold Table Structure
--------------------------------------------------------------------------------

DESC TABLE PROVIDER_GOLD;

--------------------------------------------------------------------------------
-- Display Gold Table Columns
--------------------------------------------------------------------------------

SHOW COLUMNS IN TABLE PROVIDER_GOLD;

--------------------------------------------------------------------------------
-- Total Records in Gold Table
--------------------------------------------------------------------------------

SELECT COUNT(*) AS TOTAL_RECORDS
FROM PROVIDER_GOLD;

--------------------------------------------------------------------------------
-- Display First 10 Records
--------------------------------------------------------------------------------

SELECT *
FROM PROVIDER_GOLD
LIMIT 10;

--------------------------------------------------------------------------------
-- Display Latest Loaded Records
--------------------------------------------------------------------------------

SELECT *
FROM PROVIDER_GOLD
ORDER BY LOAD_TIMESTAMP DESC;

--------------------------------------------------------------------------------
-- Latest Batch Loaded
--------------------------------------------------------------------------------

SELECT DISTINCT

BATCH_ID,
FILE_NAME,
LOAD_DATE

FROM PROVIDER_GOLD

ORDER BY LOAD_DATE DESC;

--------------------------------------------------------------------------------
-- Provider Count by Source System
--------------------------------------------------------------------------------

SELECT

SOURCE_SYSTEM,
COUNT(*) AS TOTAL_RECORDS

FROM PROVIDER_GOLD

GROUP BY SOURCE_SYSTEM;

--------------------------------------------------------------------------------
-- Provider Count by Country
--------------------------------------------------------------------------------

SELECT

CountryCode,
COUNT(*) AS TOTAL_RECORDS

FROM PROVIDER_GOLD

GROUP BY CountryCode

ORDER BY TOTAL_RECORDS DESC;

--------------------------------------------------------------------------------
-- Provider Count by State
--------------------------------------------------------------------------------

SELECT

State,
COUNT(*) AS TOTAL_RECORDS

FROM PROVIDER_GOLD

GROUP BY State

ORDER BY TOTAL_RECORDS DESC;

--------------------------------------------------------------------------------
-- Duplicate Provider Check
--------------------------------------------------------------------------------

SELECT

ProviderID,
COUNT(*) AS DUPLICATE_COUNT

FROM PROVIDER_GOLD

GROUP BY ProviderID

HAVING COUNT(*) > 1;

--------------------------------------------------------------------------------
-- Business Rule Validation Report
--------------------------------------------------------------------------------

SELECT

BUSINESS_RULE_STATUS,
COUNT(*) AS TOTAL_RECORDS

FROM PROVIDER_GOLD

GROUP BY BUSINESS_RULE_STATUS;

--------------------------------------------------------------------------------
-- Data Quality Warning Report
--------------------------------------------------------------------------------

SELECT

DATA_QUALITY_WARNING,
COUNT(*) AS TOTAL_RECORDS

FROM PROVIDER_GOLD

GROUP BY DATA_QUALITY_WARNING;

--------------------------------------------------------------------------------
-- Recently Loaded Providers
--------------------------------------------------------------------------------

SELECT

ProviderID,
FullName,
LOAD_TIMESTAMP

FROM PROVIDER_GOLD

ORDER BY LOAD_TIMESTAMP DESC;

--------------------------------------------------------------------------------
-- Check Stream Status
--------------------------------------------------------------------------------

USE SCHEMA LANDING_SC;

SELECT SYSTEM$STREAM_HAS_DATA('PROVIDER_RAW_STREAM');

--------------------------------------------------------------------------------
-- Pending Records Inside Stream
--------------------------------------------------------------------------------

SELECT COUNT(*) AS PENDING_STREAM_RECORDS

FROM PROVIDER_RAW_STREAM;

--------------------------------------------------------------------------------
-- View Stream Records Waiting for Task
--------------------------------------------------------------------------------

SELECT *

FROM PROVIDER_RAW_STREAM;

--------------------------------------------------------------------------------
-- Display Task Details
--------------------------------------------------------------------------------

SHOW TASKS;

--------------------------------------------------------------------------------
-- Describe Task
--------------------------------------------------------------------------------

DESC TASK LOAD_PROVIDER_GOLD_TASK;

--------------------------------------------------------------------------------
-- Task Execution History
--------------------------------------------------------------------------------

SELECT *

FROM TABLE
(
INFORMATION_SCHEMA.TASK_HISTORY
(
TASK_NAME=>'LOAD_PROVIDER_GOLD_TASK'
)
);

--------------------------------------------------------------------------------
-- Warehouse Status
--------------------------------------------------------------------------------

SHOW WAREHOUSES;

--------------------------------------------------------------------------------
-- Display Current Warehouse
--------------------------------------------------------------------------------

SELECT CURRENT_WAREHOUSE();

--------------------------------------------------------------------------------
-- Display Current Role
--------------------------------------------------------------------------------

SELECT CURRENT_ROLE();

--------------------------------------------------------------------------------
-- Display Current Database
--------------------------------------------------------------------------------

SELECT CURRENT_DATABASE();

--------------------------------------------------------------------------------
-- Display Current Schema
--------------------------------------------------------------------------------

SELECT CURRENT_SCHEMA();