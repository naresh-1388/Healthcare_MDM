
Part 1 : Database | Schema | Warehouse

  1. Database               ........ Line 15
  2. Schema                 ........ Line 40
  3. Warehouse              ........ Line 68

------------------------------------------------------------

Part 2 : Storage Integration | File Format | Stage

  4. Storage Integration    ........ Line 105
  5. File Format            ........ Line 130
  6. External Stage         ........ Line 150

------------------------------------------------------------

Part 3 : Snowpipe | Landing Table | Gold Table

  7. Snowpipe              ........ Line 180
  8. Landing Table         ........ Line 225
  9. Gold Table            ........ Line 265

------------------------------------------------------------

Part 4 : Stream | Task | Credit Saving

 10. Stream                ........ Line 310
 11. Task                  ........ Line 340
 12. Credit Saving         ........ Line 385

------------------------------------------------------------

Part 5 : Monitoring | Useful Commands

 13. Monitoring            ........ Line 410
 14. Useful Commands       ........ Line 460


========================================================================================
USE ROLE ACCOUNTADMIN;

===============================================================================
DATABASE
===============================================================================

-- Create Database
CREATE DATABASE ORIEO_DB;

-- Show Databases
SHOW DATABASES;

-- Use Database
USE DATABASE ORIEO_DB;

-- Describe Database
DESC DATABASE ORIEO_DB;

-- Current Database
SELECT CURRENT_DATABASE();

-- Drop Database
DROP DATABASE IF EXISTS ORIEO_DB;

===============================================================================
SCHEMA
===============================================================================

-- Create Schema
CREATE SCHEMA LANDING_SC;
CREATE SCHEMA GOLD_SC;
CREATE SCHEMA ANALYTICS_SC;

-- Show Schemas
SHOW SCHEMAS;

-- Use Schema
USE SCHEMA LANDING_SC;

-- Current Schema
SELECT CURRENT_SCHEMA();

-- Drop Schema
DROP SCHEMA IF EXISTS LANDING_SC;
DROP SCHEMA IF EXISTS GOLD_SC;
DROP SCHEMA IF EXISTS ANALYTICS_SC;

===============================================================================
WAREHOUSE
===============================================================================

-- Create Warehouse
CREATE WAREHOUSE ORIEO_WH
WITH
WAREHOUSE_SIZE='XSMALL'
AUTO_SUSPEND=60
AUTO_RESUME=TRUE;

-- Show Warehouses
SHOW WAREHOUSES;

-- Use Warehouse
USE WAREHOUSE ORIEO_WH;

-- Current Warehouse
SELECT CURRENT_WAREHOUSE();

-- Resume Warehouse
ALTER WAREHOUSE ORIEO_WH RESUME;

-- Suspend Warehouse (Save Credits)
ALTER WAREHOUSE ORIEO_WH SUSPEND;

-- Describe Warehouse
SHOW WAREHOUSES LIKE 'ORIEO_WH';

-- Drop Warehouse
DROP WAREHOUSE IF EXISTS ORIEO_WH;


======================================================================================================================================================================================================================================================================================================

===============================================================================
STORAGE INTEGRATION
===============================================================================

-- Create Storage Integration
CREATE STORAGE INTEGRATION ORIEO_INTEGRATION
TYPE=EXTERNAL_STAGE
STORAGE_PROVIDER='S3'
ENABLED=TRUE
STORAGE_AWS_ROLE_ARN='YOUR_ROLE'
STORAGE_ALLOWED_LOCATIONS=('s3://healthcare-mdm/gold/');

-- Show Storage Integrations
SHOW STORAGE INTEGRATIONS;

-- Describe Storage Integration
DESC STORAGE INTEGRATION ORIEO_INTEGRATION;

-- Drop Storage Integration
DROP STORAGE INTEGRATION IF EXISTS ORIEO_INTEGRATION;

===============================================================================
FILE FORMAT
===============================================================================

-- Create File Format
CREATE FILE FORMAT ORIEO_FF
TYPE=PARQUET;

-- Show File Formats
SHOW FILE FORMATS;

-- Describe File Format
DESC FILE FORMAT ORIEO_FF;

-- Drop File Format
DROP FILE FORMAT IF EXISTS ORIEO_FF;

===============================================================================
EXTERNAL STAGE
===============================================================================

-- Create Stage
CREATE STAGE ORIEO_STAGE
STORAGE_INTEGRATION=ORIEO_INTEGRATION
URL='s3://healthcare-mdm/gold/'
FILE_FORMAT=ORIEO_FF;

-- Show Stages
SHOW STAGES;

-- Describe Stage
DESC STAGE ORIEO_STAGE;

-- List Files
LIST @ORIEO_STAGE;

-- Remove Stage
DROP STAGE IF EXISTS ORIEO_STAGE;

======================================================================================================================================================================================================================================================================================================


===============================================================================
SNOWPIPE
===============================================================================

-- Create Pipe
CREATE OR REPLACE PIPE ORIEO_PIPE
AUTO_INGEST=TRUE
AS
COPY INTO PROVIDER_RAW
FROM @ORIEO_STAGE
FILE_FORMAT=(FORMAT_NAME=ORIEO_FF)
MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
ON_ERROR=CONTINUE;

-- Show Pipes
SHOW PIPES;

-- Describe Pipe
DESC PIPE ORIEO_PIPE;

-- Pipe Status
SELECT SYSTEM$PIPE_STATUS('ORIEO_PIPE');

-- Refresh Pipe (Load Existing Files)
ALTER PIPE ORIEO_PIPE REFRESH;

-- Copy History
SELECT *
FROM TABLE
(
INFORMATION_SCHEMA.COPY_HISTORY
(
TABLE_NAME=>'PROVIDER_RAW',
START_TIME=>DATEADD('DAY',-7,CURRENT_TIMESTAMP())
)
);

-- Drop Pipe
DROP PIPE IF EXISTS ORIEO_PIPE;

===============================================================================
LANDING TABLE
===============================================================================

-- Show Tables
SHOW TABLES;

-- Describe Table
DESC TABLE PROVIDER_RAW;

-- Show Columns
SHOW COLUMNS IN TABLE PROVIDER_RAW;

-- View Records
SELECT * FROM PROVIDER_RAW;

-- First 10 Records
SELECT * FROM PROVIDER_RAW LIMIT 10;

-- Total Records
SELECT COUNT(*) FROM PROVIDER_RAW;

-- Latest Records
SELECT * FROM PROVIDER_RAW
ORDER BY LOAD_TIMESTAMP DESC;

-- Remove Data
TRUNCATE TABLE PROVIDER_RAW;

-- Drop Table
DROP TABLE IF EXISTS PROVIDER_RAW;

===============================================================================
GOLD TABLE
===============================================================================

-- Show Tables
SHOW TABLES;

-- Describe Table
DESC TABLE PROVIDER_GOLD;

-- Show Columns
SHOW COLUMNS IN TABLE PROVIDER_GOLD;

-- View Records
SELECT * FROM PROVIDER_GOLD;

-- First 10 Records
SELECT * FROM PROVIDER_GOLD LIMIT 10;

-- Total Records
SELECT COUNT(*) FROM PROVIDER_GOLD;

-- Latest Records
SELECT * FROM PROVIDER_GOLD
ORDER BY LOAD_TIMESTAMP DESC;

-- Remove Data
TRUNCATE TABLE PROVIDER_GOLD;

-- Drop Table
DROP TABLE IF EXISTS PROVIDER_GOLD;


======================================================================================================================================================================================================================================================================================================

===============================================================================
STREAM
===============================================================================

-- Create Stream
CREATE STREAM PROVIDER_RAW_STREAM
ON TABLE PROVIDER_RAW;

-- Show Streams
SHOW STREAMS;

-- Describe Stream
DESC STREAM PROVIDER_RAW_STREAM;

-- Stream Status
SELECT SYSTEM$STREAM_HAS_DATA('PROVIDER_RAW_STREAM');

-- View Stream Records
SELECT * FROM PROVIDER_RAW_STREAM;

-- Pending Records
SELECT COUNT(*) FROM PROVIDER_RAW_STREAM;

-- Drop Stream
DROP STREAM IF EXISTS PROVIDER_RAW_STREAM;

===============================================================================
TASK
===============================================================================

-- Show Tasks
SHOW TASKS;

-- Describe Task
DESC TASK LOAD_PROVIDER_GOLD_TASK;

-- Resume Task
ALTER TASK LOAD_PROVIDER_GOLD_TASK RESUME;

-- Suspend Task (Save Credits)
ALTER TASK LOAD_PROVIDER_GOLD_TASK SUSPEND;

-- Execute Immediately
EXECUTE TASK LOAD_PROVIDER_GOLD_TASK;

-- Task History
SELECT *
FROM TABLE
(
INFORMATION_SCHEMA.TASK_HISTORY
(
TASK_NAME=>'LOAD_PROVIDER_GOLD_TASK'
)
);

-- Drop Task
DROP TASK IF EXISTS LOAD_PROVIDER_GOLD_TASK;

===============================================================================
CREDIT SAVING
===============================================================================

-- Suspend Warehouse
ALTER WAREHOUSE ORIEO_WH SUSPEND;

-- Resume Warehouse
ALTER WAREHOUSE ORIEO_WH RESUME;

-- Current Warehouse
SELECT CURRENT_WAREHOUSE();

-- Show Warehouses
SHOW WAREHOUSES;

======================================================================================================================================================================================================================================================================================================

===============================================================================
MONITORING
===============================================================================

-- Current Role
SELECT CURRENT_ROLE();

-- Current Database
SELECT CURRENT_DATABASE();

-- Current Schema
SELECT CURRENT_SCHEMA();

-- Current Warehouse
SELECT CURRENT_WAREHOUSE();

-- Landing Record Count
SELECT COUNT(*) FROM ORIEO_DB.LANDING_SC.PROVIDER_RAW;

-- Gold Record Count
SELECT COUNT(*) FROM ORIEO_DB.GOLD_SC.PROVIDER_GOLD;

-- Stage Files
LIST @ORIEO_STAGE;

-- Pipe Status
SELECT SYSTEM$PIPE_STATUS('ORIEO_PIPE');

-- Stream Status
SELECT SYSTEM$STREAM_HAS_DATA('PROVIDER_RAW_STREAM');

-- Copy History
SELECT *
FROM TABLE
(
INFORMATION_SCHEMA.COPY_HISTORY
(
TABLE_NAME=>'PROVIDER_RAW',
START_TIME=>DATEADD('DAY',-7,CURRENT_TIMESTAMP())
)
);

-- Task History
SELECT *
FROM TABLE
(
INFORMATION_SCHEMA.TASK_HISTORY
(
TASK_NAME=>'LOAD_PROVIDER_GOLD_TASK'
)
);

-- Query History
SELECT *
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
ORDER BY START_TIME DESC
LIMIT 20;

===============================================================================
USEFUL COMMANDS
===============================================================================

-- Current User
SELECT CURRENT_USER();

-- Current Account
SELECT CURRENT_ACCOUNT();

-- Current Version
SELECT CURRENT_VERSION();

-- Current Timestamp
SELECT CURRENT_TIMESTAMP();

-- Current Region
SELECT CURRENT_REGION();

-- Current Available Roles
SHOW ROLES;

-- Current Grants
SHOW GRANTS;

-- Login History
SELECT *
FROM TABLE(INFORMATION_SCHEMA.LOGIN_HISTORY())
LIMIT 20;

======================================================================================================================================================================================================================================================================================================


