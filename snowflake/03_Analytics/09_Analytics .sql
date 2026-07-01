Project         : Healthcare Master Data Management (Healthcare_MDM)
Author          : Naresh
Layer           : Analytics Layer

Purpose
-------
This file contains business analytical queries executed on the Gold Layer. These queries help Business Analysts and Management
understand provider distribution, data quality, business rules and operational insights.

******************************************************************************/

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE ORIEO_WH;
USE DATABASE ORIEO_DB;
USE SCHEMA GOLD_SC;

/******************************************************************************
Business Analytics
Part 1A : Business Questions 1 - 9
******************************************************************************/

-- Business Question 1
/*
Purpose:
Find the total number of provider records available in the Gold layer.
This helps validate whether all provider records were loaded successfully.
*/
SELECT COUNT(*) AS TOTAL_PROVIDERS FROM PROVIDER_GOLD;


---------------------------------------------------------------------------------

-- Business Question 2
/*
Purpose:
Display the number of Active and Inactive providers.
Useful for understanding the operational status of providers.
*/
SELECT Status,COUNT(*) AS TOTAL_PROVIDERS
FROM PROVIDER_GOLD
GROUP BY Status
ORDER BY TOTAL_PROVIDERS DESC;


---------------------------------------------------------------------------------

-- Business Question 3
/*
Purpose:
Identify which States have the highest number of providers.
This helps understand provider distribution geographically.
*/
SELECT State,COUNT(*) AS TOTAL_PROVIDERS
FROM PROVIDER_GOLD
GROUP BY State
ORDER BY TOTAL_PROVIDERS DESC;


---------------------------------------------------------------------------------

-- Business Question 4
/*
Purpose:
Show provider distribution across different countries.
Useful when providers are loaded from multiple countries.
*/
SELECT CountryCode,COUNT(*) AS TOTAL_PROVIDERS
FROM PROVIDER_GOLD
GROUP BY CountryCode
ORDER BY TOTAL_PROVIDERS DESC;


---------------------------------------------------------------------------------

-- Business Question 5
/*
Purpose:
Display provider count based on Gender.
Useful for demographic and diversity reporting.
*/
SELECT Gender,COUNT(*) AS TOTAL_PROVIDERS
FROM PROVIDER_GOLD
GROUP BY Gender
ORDER BY TOTAL_PROVIDERS DESC;


---------------------------------------------------------------------------------

-- Business Question 6
/*
Purpose:
Display provider distribution by Credential.
Helps identify qualification and certification categories.
*/
SELECT Credential,COUNT(*) AS TOTAL_PROVIDERS
FROM PROVIDER_GOLD
GROUP BY Credential
ORDER BY TOTAL_PROVIDERS DESC;


---------------------------------------------------------------------------------

-- Business Question 7
/*
Purpose:
Find Cities having the highest number of providers.
Useful for regional healthcare planning and reporting.
*/
SELECT City,COUNT(*) AS TOTAL_PROVIDERS
FROM PROVIDER_GOLD
GROUP BY City
ORDER BY TOTAL_PROVIDERS DESC;


---------------------------------------------------------------------------------
-- Business Question 8
/*
Purpose:
Summarize Business Rule Validation results.
Helps identify valid and invalid provider records.
*/
SELECT BUSINESS_RULE_STATUS,COUNT(*) AS TOTAL_RECORDS
FROM PROVIDER_GOLD
GROUP BY BUSINESS_RULE_STATUS
ORDER BY TOTAL_RECORDS DESC;


----------------------------------------------------------------------------------
-- Business Question 9
/*
Purpose:
Display Data Quality Warning summary.
Helps identify records requiring data quality improvements.
*/
SELECT DATA_QUALITY_WARNING,COUNT(*) AS TOTAL_RECORDS
FROM PROVIDER_GOLD
GROUP BY DATA_QUALITY_WARNING
ORDER BY TOTAL_RECORDS DESC;

---------------------------------------------------------------------------------

/******************************************************************************
Business Analytics
Part 1B : Business Questions 10 - 18
******************************************************************************/

-- Business Question 10
/*
Purpose:
Display the latest batch loaded into the Gold table.
Useful for validating the most recent pipeline execution.
*/
SELECT DISTINCT BATCH_ID,FILE_NAME,LOAD_DATE
FROM PROVIDER_GOLD
ORDER BY LOAD_DATE DESC;


---------------------------------------------------------------------------------

-- Business Question 11
/*
Purpose:
Display provider count by Source System.
Useful when data is received from multiple source applications.
*/
SELECT SOURCE_SYSTEM,COUNT(*) AS TOTAL_PROVIDERS
FROM PROVIDER_GOLD
GROUP BY SOURCE_SYSTEM
ORDER BY TOTAL_PROVIDERS DESC;


---------------------------------------------------------------------------------

-- Business Question 12
/*
Purpose:
Display provider count by Payload Format.
Helps identify different incoming data formats.
*/
SELECT PAYLOAD_FORMAT,COUNT(*) AS TOTAL_PROVIDERS
FROM PROVIDER_GOLD
GROUP BY PAYLOAD_FORMAT
ORDER BY TOTAL_PROVIDERS DESC;


---------------------------------------------------------------------------------

-- Business Question 13
/*
Purpose:
Display provider count by Target System.
Useful for validating data delivered to downstream systems.
*/
SELECT TARGET_SYSTEM,COUNT(*) AS TOTAL_PROVIDERS
FROM PROVIDER_GOLD
GROUP BY TARGET_SYSTEM
ORDER BY TOTAL_PROVIDERS DESC;


---------------------------------------------------------------------------------

-- Business Question 14
/*
Purpose:
Display recently loaded providers.
Useful for validating the latest records loaded into Gold.
*/
SELECT ProviderID,FullName,State,LOAD_TIMESTAMP
FROM PROVIDER_GOLD
ORDER BY LOAD_TIMESTAMP DESC;


---------------------------------------------------------------------------------

-- Business Question 15
/*
Purpose:
Identify duplicate Provider IDs.
Duplicate records indicate data quality or ingestion issues.
*/
SELECT ProviderID,COUNT(*) AS DUPLICATE_COUNT
FROM PROVIDER_GOLD
GROUP BY ProviderID
HAVING COUNT(*)>1;


---------------------------------------------------------------------------------

-- Business Question 16
/*
Purpose:
Find providers with missing Phone Numbers.
Useful for identifying incomplete provider information.
*/
SELECT *
FROM PROVIDER_GOLD
WHERE PhoneNumber IS NULL;


---------------------------------------------------------------------------------

-- Business Question 17
/*
Purpose:
Find providers with missing License Numbers.
Useful for validating mandatory provider information.
*/
SELECT *
FROM PROVIDER_GOLD
WHERE LicenseNumber IS NULL;


---------------------------------------------------------------------------------

-- Business Question 18
/*
Purpose:
Find providers with missing Taxonomy Codes.
Useful for identifying incomplete provider classification data.
*/
SELECT *
FROM PROVIDER_GOLD
WHERE TaxonomyCode IS NULL;

---------------------------------------------------------------------------------


/******************************************************************************
Business Analytics
Part 2 : Business Questions 19 - 35
******************************************************************************/

-- Business Question 19
/*
Purpose:
Display complete provider information.
Useful for validating the final curated Gold dataset.
*/
SELECT *
FROM PROVIDER_GOLD
ORDER BY FullName;


---------------------------------------------------------------------------------

-- Business Question 20
/*
Purpose:
Display total provider records loaded in each Batch.
Useful for validating every pipeline execution.
*/
SELECT BATCH_ID,COUNT(*) AS TOTAL_RECORDS
FROM PROVIDER_GOLD
GROUP BY BATCH_ID
ORDER BY TOTAL_RECORDS DESC;


---------------------------------------------------------------------------------

-- Business Question 21
/*
Purpose:
Display provider distribution by Taxonomy Code.
Helps understand provider specialization categories.
*/
SELECT TaxonomyCode,COUNT(*) AS TOTAL_PROVIDERS
FROM PROVIDER_GOLD
GROUP BY TaxonomyCode
ORDER BY TOTAL_PROVIDERS DESC;


---------------------------------------------------------------------------------

-- Business Question 22
/*
Purpose:
Display provider count by Postal Code.
Useful for geographical distribution analysis.
*/
SELECT PostalCode,COUNT(*) AS TOTAL_PROVIDERS
FROM PROVIDER_GOLD
GROUP BY PostalCode
ORDER BY TOTAL_PROVIDERS DESC;


---------------------------------------------------------------------------------

-- Business Question 23
/*
Purpose:
Display provider count by License Number.
Useful for validating provider licensing information.
*/
SELECT LicenseNumber,COUNT(*) AS TOTAL_PROVIDERS
FROM PROVIDER_GOLD
GROUP BY LicenseNumber
ORDER BY TOTAL_PROVIDERS DESC;


---------------------------------------------------------------------------------

-- Business Question 24
/*
Purpose:
Display provider count by File Name.
Useful for validating records loaded from each source file.
*/
SELECT FILE_NAME,COUNT(*) AS TOTAL_RECORDS
FROM PROVIDER_GOLD
GROUP BY FILE_NAME
ORDER BY TOTAL_RECORDS DESC;


---------------------------------------------------------------------------------

-- Business Question 25
/*
Purpose:
Display provider count loaded on each Load Date.
Useful for monitoring daily data ingestion.
*/
SELECT LOAD_DATE,COUNT(*) AS TOTAL_RECORDS
FROM PROVIDER_GOLD
GROUP BY LOAD_DATE
ORDER BY LOAD_DATE DESC;


---------------------------------------------------------------------------------

-- Business Question 26
/*
Purpose:
Display the latest five provider records loaded into Gold.
Useful for validating recent pipeline executions.
*/
SELECT *
FROM PROVIDER_GOLD
ORDER BY LOAD_TIMESTAMP DESC
LIMIT 5;


---------------------------------------------------------------------------------

-- Business Question 27
/*
Purpose:
Identify providers with missing Address information.
Useful for improving address data quality.
*/
SELECT *
FROM PROVIDER_GOLD
WHERE AddressLine1 IS NULL;


---------------------------------------------------------------------------------

-- Business Question 28
/*
Purpose:
Identify providers with missing Identifier values.
Useful for validating provider master data completeness.
*/
SELECT *
FROM PROVIDER_GOLD
WHERE Identifier IS NULL;


---------------------------------------------------------------------------------

-- Business Question 29
/*
Purpose:
Display all unique States available in the Gold layer.
Useful for quick reference and validation.
*/
SELECT DISTINCT State
FROM PROVIDER_GOLD
ORDER BY State;


---------------------------------------------------------------------------------

-- Business Question 30
/*
Purpose:
Display all unique Cities available in the Gold layer.
Useful for validating geographical coverage.
*/
SELECT DISTINCT City
FROM PROVIDER_GOLD
ORDER BY City;


---------------------------------------------------------------------------------

-- Business Question 31
/*
Purpose:
Display all unique Credentials assigned to providers.
Useful for qualification analysis.
*/
SELECT DISTINCT Credential
FROM PROVIDER_GOLD
ORDER BY Credential;


---------------------------------------------------------------------------------

-- Business Question 32
/*
Purpose:
Display all unique Taxonomy Codes available.
Useful for provider specialization validation.
*/
SELECT DISTINCT TaxonomyCode
FROM PROVIDER_GOLD
ORDER BY TaxonomyCode;


---------------------------------------------------------------------------------

-- Business Question 33
/*
Purpose:
Display Business Rule Status across different States.
Useful for identifying region-wise validation issues.
*/
SELECT State,BUSINESS_RULE_STATUS,COUNT(*) AS TOTAL_RECORDS
FROM PROVIDER_GOLD
GROUP BY State,BUSINESS_RULE_STATUS
ORDER BY State;


---------------------------------------------------------------------------------

-- Business Question 34
/*
Purpose:
Display Data Quality Warnings across different States.
Useful for identifying areas requiring data cleansing.
*/
SELECT State,DATA_QUALITY_WARNING,COUNT(*) AS TOTAL_RECORDS
FROM PROVIDER_GOLD
GROUP BY State,DATA_QUALITY_WARNING
ORDER BY State;


---------------------------------------------------------------------------------

-- Business Question 35
/*
Purpose:
Display provider count by State and Gender.
Useful for demographic and regional reporting.
*/
SELECT State,Gender,COUNT(*) AS TOTAL_PROVIDERS
FROM PROVIDER_GOLD
GROUP BY State,Gender
ORDER BY State,TOTAL_PROVIDERS DESC;