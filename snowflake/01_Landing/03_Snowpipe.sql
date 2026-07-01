/******************************************************************************
Project         : Healthcare Master Data Management (Healthcare_MDM)

File Name       : 03_Snowpipe.sql

Author          : Naresh

Layer           : Landing Layer

Object Type     : Snowpipe

Object Name     : ORIEO_PIPE

Purpose
-------
Snowpipe automatically loads new Parquet files arriving in the
AWS S3 bucket into the Landing table (PROVIDER_RAW).

Whenever a new file is uploaded to the configured S3 location,
Snowpipe copies only the new data into Snowflake.

This enables an automated ingestion pipeline without manually
executing COPY INTO commands.

Pipeline Flow
-------------
AWS S3
   ↓
External Stage
   ↓
Snowpipe
   ↓
LANDING_SC.PROVIDER_RAW

Business Benefits
-----------------
• Fully automated data ingestion.
• Loads only newly arrived files.
• Supports near real-time processing.
• Eliminates manual data loading.
• Foundation for Streams and Tasks.

******************************************************************************/

USE ROLE ACCOUNTADMIN;

USE WAREHOUSE ORIEO_WH;

USE DATABASE ORIEO_DB;

USE SCHEMA LANDING_SC;

--------------------------------------------------------------------------------
-- Create Snowpipe
--------------------------------------------------------------------------------
-- MATCH_BY_COLUMN_NAME
-- --------------------
-- Since the source files are Parquet, Snowflake matches
-- Parquet column names with table column names.
--
-- CASE_INSENSITIVE means
-- ProviderID
-- providerid
-- PROVIDERID
-- are treated as the same column.
--------------------------------------------------------------------------------

CREATE OR REPLACE PIPE ORIEO_PIPE
AUTO_INGEST = TRUE
AS

COPY INTO PROVIDER_RAW
FROM @ORIEO_STAGE

FILE_FORMAT = (FORMAT_NAME = ORIEO_FF)

MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE

ON_ERROR = CONTINUE;



--------------------------------------------------------------------------------
-- Display Pipe Information
--------------------------------------------------------------------------------

SHOW PIPES;

--------------------------------------------------------------------------------
-- Describe Pipe
--------------------------------------------------------------------------------

DESC PIPE ORIEO_PIPE;

--------------------------------------------------------------------------------
-- View Current Pipe Status
--------------------------------------------------------------------------------

SELECT SYSTEM$PIPE_STATUS('ORIEO_PIPE');

--------------------------------------------------------------------------------
-- Refresh Snowpipe
--------------------------------------------------------------------------------
-- Use this command after creating the pipe if files already
-- exist in the stage.
--
-- Snowpipe scans the stage and loads files that have not yet
-- been processed.
--------------------------------------------------------------------------------

ALTER PIPE ORIEO_PIPE REFRESH;

--------------------------------------------------------------------------------
-- Verify Data Loaded
--------------------------------------------------------------------------------

SELECT COUNT(*) AS TOTAL_RECORDS
FROM PROVIDER_RAW;

--------------------------------------------------------------------------------
-- View Loaded Records
--------------------------------------------------------------------------------

SELECT *
FROM PROVIDER_RAW
LIMIT 10;

--------------------------------------------------------------------------------
-- View Latest Loaded Records
--------------------------------------------------------------------------------

SELECT *
FROM PROVIDER_RAW
ORDER BY LOAD_TIMESTAMP DESC;

--------------------------------------------------------------------------------
-- Snowpipe Load History
--------------------------------------------------------------------------------
-- Displays which files were loaded successfully.
--------------------------------------------------------------------------------

SELECT *
FROM TABLE(

INFORMATION_SCHEMA.COPY_HISTORY(

TABLE_NAME => 'PROVIDER_RAW',

START_TIME => DATEADD('DAY',-7,CURRENT_TIMESTAMP())

)

);

--------------------------------------------------------------------------------
-- Pipe Status
--------------------------------------------------------------------------------

SELECT SYSTEM$PIPE_STATUS('ORIEO_PIPE');

--------------------------------------------------------------------------------
-- List Files Available in Stage
--------------------------------------------------------------------------------

LIST @ORIEO_STAGE;

--------------------------------------------------------------------------------
-- Remove Snowpipe
--------------------------------------------------------------------------------
-- Use only during development.
--------------------------------------------------------------------------------

DROP PIPE IF EXISTS ORIEO_PIPE;