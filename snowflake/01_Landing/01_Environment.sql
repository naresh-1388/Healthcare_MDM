-- Healthcare MDM pipeline: database, schemas, warehouse, S3 integration, and stage setup
-- Co-authored with CoCo

-- Create Database 
CREATE DATABASE Orieo_DB;

USE database Orieo_DB;

--------------------------------------------------------------------------------
-- Create schemas
CREATE SCHEMA IF NOT EXISTS LANDING_SC;  -- Raw from S3
CREATE SCHEMA IF NOT EXISTS GOLD_SC;      -- Final tables
CREATE SCHEMA IF NOT EXISTS ANALYTICS_SC; -- dbt models

USE Schema Landing_SC;

----------------------------------------------------------------------------------
-- Creating Warehouse 
CREATE WAREHOUSE Orieo_WH
WITH
WAREHOUSE_SIZE = 'XSMALL'
AUTO_SUSPEND = 60
AUTO_RESUME = TRUE;


use warehouse Orieo_WH

-----------------------------------------------------------------------------------


-- Create Storage Integration
CREATE STORAGE INTEGRATION Orieo_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::169237360936:role/healthcare-mdm-iam-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://healthcare-mdm/gold/');


-------------------------------------------------------------------------------------
-- Get Snowflake ARNs (copy these to AWS IAM Role trust relationship)
DESC STORAGE INTEGRATION Orieo_integration;
-- Note: STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID



--------------------------------------------------------------------------------------

-- File Format
CREATE FILE FORMAT Orieo_FF
TYPE = PARQUET;

-------------------------------------------------------------------------------------
-- Create External Stage
CREATE STAGE Orieo_stage
  STORAGE_INTEGRATION = Orieo_integration
  URL = 's3://healthcare-mdm/gold/'
  FILE_FORMAT = Orieo_FF;


-- Stages list 
LIST @ORIEO_STAGE;

-- ====================================================================================


