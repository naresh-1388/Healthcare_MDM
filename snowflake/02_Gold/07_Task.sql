/******************************************************************************
Project         : Healthcare Master Data Management (Healthcare_MDM)

Author          : Naresh

Layer           : Gold Layer

Object Type     : Task

Object Name     : LOAD_PROVIDER_GOLD_TASK

Purpose
-------
This Task automatically reads newly arrived provider records from the
Landing Stream and merges them into the Gold table.

The task performs Incremental Loading (CDC).

Business Logic
--------------
1. Read only NEW records from PROVIDER_RAW_STREAM.
2. If ProviderID already exists in Gold table
      → UPDATE existing record.
3. If ProviderID does not exist
      → INSERT new record.

Benefits
--------
• Prevents duplicate ProviderIDs.
• Supports Incremental Data Loading.
• Eliminates Full Table Scan.
• Suitable for Production ETL Pipelines.

Pipeline
--------
AWS S3
   ↓
Snowpipe
   ↓
LANDING_SC.PROVIDER_RAW
   ↓
PROVIDER_RAW_STREAM
   ↓
LOAD_PROVIDER_GOLD_TASK
   ↓
GOLD_SC.PROVIDER_GOLD

******************************************************************************/

USE ROLE ACCOUNTADMIN;

USE WAREHOUSE ORIEO_WH;

USE DATABASE ORIEO_DB;

USE SCHEMA LANDING_SC;


--------------------------------------------------------------------------------
-- Create Task
--
-- This Task executes every 1 minute.
--
-- If Stream contains no new rows,
-- MERGE simply performs no changes.
--
-- If Stream contains new provider records,
-- they are merged into the Gold table.
--------------------------------------------------------------------------------

CREATE OR REPLACE TASK LOAD_PROVIDER_GOLD_TASK

WAREHOUSE = ORIEO_WH

WHEN
SYSTEM$STREAM_HAS_DATA('PROVIDER_RAW_STREAM')

AS

MERGE INTO ORIEO_DB.GOLD_SC.PROVIDER_GOLD T

USING ORIEO_DB.LANDING_SC.PROVIDER_RAW_STREAM S

ON T.ProviderID = S.ProviderID

WHEN MATCHED THEN
UPDATE SET

T.FullName=S.FullName,
T.Credential=S.Credential,
T.Gender=S.Gender,
T.Status=S.Status,
T.AddressLine1=S.AddressLine1,
T.City=S.City,
T.State=S.State,
T.PostalCode=S.PostalCode,
T.CountryCode=S.CountryCode,
T.PhoneNumber=S.PhoneNumber,
T.TaxonomyCode=S.TaxonomyCode,
T.LicenseNumber=S.LicenseNumber,
T.Identifier=S.Identifier,
T.BUSINESS_RULE_STATUS=S.BUSINESS_RULE_STATUS,
T.DATA_QUALITY_WARNING=S.DATA_QUALITY_WARNING,
T.SOURCE_SYSTEM=S.SOURCE_SYSTEM,
T.PAYLOAD_FORMAT=S.PAYLOAD_FORMAT,
T.TARGET_SYSTEM=S.TARGET_SYSTEM,
T.LOAD_TIMESTAMP=S.LOAD_TIMESTAMP,
T.LOAD_DATE=S.LOAD_DATE,
T.FILE_NAME=S.FILE_NAME,
T.BATCH_ID=S.BATCH_ID

WHEN NOT MATCHED THEN

INSERT
(
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
)

VALUES
(
S.ProviderID,
S.FullName,
S.Credential,
S.Gender,
S.Status,
S.AddressLine1,
S.City,
S.State,
S.PostalCode,
S.CountryCode,
S.PhoneNumber,
S.TaxonomyCode,
S.LicenseNumber,
S.Identifier,
S.BUSINESS_RULE_STATUS,
S.DATA_QUALITY_WARNING,
S.SOURCE_SYSTEM,
S.PAYLOAD_FORMAT,
S.TARGET_SYSTEM,
S.LOAD_TIMESTAMP,
S.LOAD_DATE,
S.FILE_NAME,
S.BATCH_ID
);

--------------------------------------------------------------------------------
-- Verify Task Created
--------------------------------------------------------------------------------

SHOW TASKS;

--------------------------------------------------------------------------------
-- Display Task Information
--------------------------------------------------------------------------------

DESC TASK LOAD_PROVIDER_GOLD_TASK;

--------------------------------------------------------------------------------
-- Resume Task
--
-- Resume enables the scheduler.
--------------------------------------------------------------------------------

ALTER TASK LOAD_PROVIDER_GOLD_TASK RESUME;

--------------------------------------------------------------------------------
-- Suspend Task
--
-- Suspend the task after testing to save Snowflake credits.
--------------------------------------------------------------------------------

ALTER TASK LOAD_PROVIDER_GOLD_TASK SUSPEND;

--------------------------------------------------------------------------------
-- Execute Task Immediately
--
-- Useful while testing.
--------------------------------------------------------------------------------

EXECUTE TASK LOAD_PROVIDER_GOLD_TASK;

--------------------------------------------------------------------------------
-- View Task Execution History
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
-- Verify Gold Table Record Count
--------------------------------------------------------------------------------

SELECT COUNT(*) AS TOTAL_RECORDS

FROM ORIEO_DB.GOLD_SC.PROVIDER_GOLD;

--------------------------------------------------------------------------------
-- View Gold Records
--------------------------------------------------------------------------------

SELECT *

FROM ORIEO_DB.GOLD_SC.PROVIDER_GOLD

LIMIT 10;

--------------------------------------------------------------------------------
-- Verify Stream Status
--------------------------------------------------------------------------------

SELECT COUNT(*)

FROM PROVIDER_RAW_STREAM;

--------------------------------------------------------------------------------
-- Check if Stream contains pending records
--------------------------------------------------------------------------------

SELECT SYSTEM$STREAM_HAS_DATA('PROVIDER_RAW_STREAM');

--------------------------------------------------------------------------------
-- Drop Task
--
-- Use only during development.
--------------------------------------------------------------------------------

DROP TASK IF EXISTS LOAD_PROVIDER_GOLD_TASK;