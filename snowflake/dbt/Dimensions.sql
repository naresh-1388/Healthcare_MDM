USE ROLE ACCOUNTADMIN;
USE WAREHOUSE ORIEO_WH;
USE DATABASE ORIEO_DB;
USE SCHEMA ANALYTICS_SC;


=================================================================

-- Show all dbt created tables
SHOW TABLES;

-- Show only Dimension tables
SHOW TABLES LIKE 'DIM%';


=================================================================
-- View Data
SELECT * FROM DIM_PROVIDER;

-- First 10 Records
SELECT * FROM DIM_PROVIDER LIMIT 10;

-- Total Records
SELECT COUNT(*) FROM DIM_PROVIDER;

-- Latest Loaded Records
SELECT *
FROM DIM_PROVIDER
ORDER BY LOAD_TIMESTAMP DESC;

-- Check Duplicate ProviderIDs
SELECT ProviderID,COUNT(*)
FROM DIM_PROVIDER
GROUP BY ProviderID
HAVING COUNT(*)>1;

-- Check NULL ProviderID
SELECT *
FROM DIM_PROVIDER
WHERE ProviderID IS NULL;

-- Check NULL TaxonomyCode
SELECT *
FROM DIM_PROVIDER
WHERE TaxonomyCode IS NULL;

-- Distinct States
SELECT DISTINCT State
FROM DIM_PROVIDER
ORDER BY State;

-- Distinct Cities
SELECT DISTINCT City
FROM DIM_PROVIDER
ORDER BY City;

-- Distinct Gender
SELECT DISTINCT Gender
FROM DIM_PROVIDER;

-- Distinct Status
SELECT DISTINCT Status
FROM DIM_PROVIDER;

-- Distinct Country
SELECT DISTINCT CountryCode
FROM DIM_PROVIDER;

-- Distinct Taxonomy
SELECT DISTINCT TaxonomyCode
FROM DIM_PROVIDER;

-- Describe Table
DESC TABLE DIM_PROVIDER;

-- Show Columns
SHOW COLUMNS IN TABLE DIM_PROVIDER;

====================================================================================================
====================================================================


--- CLeanup 

DROP TABLE IF EXISTS DIM_PROVIDER;


TRUNCATE TABLE DIM_PROVIDER;



---------------------------------

SELECT BUSINESS_RULE_STATUS,COUNT(*)
FROM ORIEO_DB.ANALYTICS_SC.MART_PROVIDER_BUSINESS_RULES
GROUP BY BUSINESS_RULE_STATUS;

SELECT *
FROM ORIEO_DB.ANALYTICS_SC.MART_PROVIDER_DATA_QUALITY;