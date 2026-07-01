{{config(materialized='table')}}

SELECT
TaxonomyCode,
COUNT(*) AS TOTAL_PROVIDERS,
COUNT(DISTINCT State) AS TOTAL_STATES,
COUNT(DISTINCT City) AS TOTAL_CITIES
FROM {{ref('dim_provider')}}
GROUP BY TaxonomyCode
ORDER BY TOTAL_PROVIDERS DESC


-- dbt run --select mart_provider_taxonomy