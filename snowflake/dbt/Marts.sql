USE ROLE ACCOUNTADMIN;
USE WAREHOUSE ORIEO_WH;
USE DATABASE ORIEO_DB;
USE SCHEMA ANALYTICS_SC;


--------------------------------------------------------
-- Show only Mart tables
SHOW TABLES LIKE 'MART%';


===============================================================================
MART_PROVIDER_STATE
===============================================================================

SELECT * FROM MART_PROVIDER_STATE;
SELECT COUNT(*) FROM MART_PROVIDER_STATE;
DESC TABLE MART_PROVIDER_STATE;
SHOW COLUMNS IN TABLE MART_PROVIDER_STATE;

===============================================================================
MART_PROVIDER_SUMMARY
===============================================================================

SELECT * FROM MART_PROVIDER_SUMMARY;
SELECT COUNT(*) FROM MART_PROVIDER_SUMMARY;
DESC TABLE MART_PROVIDER_SUMMARY;
SHOW COLUMNS IN TABLE MART_PROVIDER_SUMMARY;

===============================================================================
MART_PROVIDER_GENDER
===============================================================================

SELECT * FROM MART_PROVIDER_GENDER;
SELECT COUNT(*) FROM MART_PROVIDER_GENDER;
DESC TABLE MART_PROVIDER_GENDER;

===============================================================================
MART_PROVIDER_TAXONOMY
===============================================================================

SELECT * FROM MART_PROVIDER_TAXONOMY;
SELECT COUNT(*) FROM MART_PROVIDER_TAXONOMY;
DESC TABLE MART_PROVIDER_TAXONOMY;

===============================================================================
MART_PROVIDER_BUSINESS_RULES
===============================================================================

SELECT * FROM MART_PROVIDER_BUSINESS_RULES;
SELECT COUNT(*) FROM MART_PROVIDER_BUSINESS_RULES;
DESC TABLE MART_PROVIDER_BUSINESS_RULES;

===============================================================================
MART_PROVIDER_DATA_QUALITY
===============================================================================

SELECT * FROM MART_PROVIDER_DATA_QUALITY;
SELECT COUNT(*) FROM MART_PROVIDER_DATA_QUALITY;
DESC TABLE MART_PROVIDER_DATA_QUALITY;

===============================================================================
DBT DATA VALIDATION
===============================================================================

-- Gold vs Dimension Count
SELECT
(SELECT COUNT(*) FROM ORIEO_DB.GOLD_SC.PROVIDER_GOLD) AS GOLD_COUNT,
(SELECT COUNT(*) FROM DIM_PROVIDER) AS DIM_COUNT;

-- State Count Validation
SELECT COUNT(DISTINCT State)
FROM DIM_PROVIDER;

-- Taxonomy Count Validation
SELECT COUNT(DISTINCT TaxonomyCode)
FROM DIM_PROVIDER;

-- City Count Validation
SELECT COUNT(DISTINCT City)
FROM DIM_PROVIDER;

-- Business Rule Summary
SELECT BUSINESS_RULE_STATUS,COUNT(*)
FROM DIM_PROVIDER
GROUP BY BUSINESS_RULE_STATUS;

-- Data Quality Summary
SELECT DATA_QUALITY_WARNING,COUNT(*)
FROM DIM_PROVIDER
GROUP BY DATA_QUALITY_WARNING;

===============================================================================
DBT OBJECTS
===============================================================================

SHOW TABLES;
SHOW VIEWS;

===============================================================================
CLEANUP
===============================================================================

-- Remove only data

TRUNCATE TABLE MART_PROVIDER_STATE;
TRUNCATE TABLE MART_PROVIDER_SUMMARY;
TRUNCATE TABLE MART_PROVIDER_GENDER;
TRUNCATE TABLE MART_PROVIDER_TAXONOMY;
TRUNCATE TABLE MART_PROVIDER_BUSINESS_RULES;
TRUNCATE TABLE MART_PROVIDER_DATA_QUALITY;

-- Drop Tables

DROP TABLE IF EXISTS MART_PROVIDER_STATE;
DROP TABLE IF EXISTS MART_PROVIDER_SUMMARY;
DROP TABLE IF EXISTS MART_PROVIDER_GENDER;
DROP TABLE IF EXISTS MART_PROVIDER_TAXONOMY;
DROP TABLE IF EXISTS MART_PROVIDER_BUSINESS_RULES;
DROP TABLE IF EXISTS MART_PROVIDER_DATA_QUALITY;