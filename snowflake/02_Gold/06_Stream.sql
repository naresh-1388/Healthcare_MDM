/******************************************************************************
Project      : Healthcare Master Data Management

File Name    : 06_Stream.sql

Layer        : Gold

Object       : PROVIDER_RAW_STREAM

Purpose
-------
This Stream captures every NEW provider record inserted into the
Landing table by Snowpipe.

Only INSERT operations are tracked.

This Stream is consumed by the Snowflake Task.

******************************************************************************/

USE ROLE ACCOUNTADMIN;

USE WAREHOUSE ORIEO_WH;

USE DATABASE ORIEO_DB;

USE SCHEMA LANDING_SC;





CREATE STREAM LANDING_SC.PROVIDER_RAW_STREAM
ON TABLE LANDING_SC.PROVIDER_RAW;



--------------------------------------------------------------------------------
-- Verify Stream
--------------------------------------------------------------------------------

SHOW STREAMS;

DESC STREAM PROVIDER_RAW_STREAM;


SELECT COUNT(*)
FROM LANDING_SC.PROVIDER_RAW_STREAM;
--------------------------------------------------------------------------------
-- View Pending Records
--------------------------------------------------------------------------------

SELECT *
FROM PROVIDER_RAW_STREAM;

--------------------------------------------------------------------------------
-- Check Stream Status
--------------------------------------------------------------------------------

SELECT SYSTEM$STREAM_HAS_DATA('PROVIDER_RAW_STREAM');

--------------------------------------------------------------------------------
-- Pending Row Count
--------------------------------------------------------------------------------

SELECT COUNT(*)
FROM PROVIDER_RAW_STREAM;



