{{config(materialized='table')}}

SELECT
Gender,
COUNT(*) AS TOTAL_PROVIDERS,
COUNT(DISTINCT State) AS TOTAL_STATES,
COUNT(DISTINCT TaxonomyCode) AS TOTAL_SPECIALIZATIONS
FROM {{ref('dim_provider')}}
GROUP BY Gender
ORDER BY TOTAL_PROVIDERS DESC




-- dbt run --select mart_provider_gender