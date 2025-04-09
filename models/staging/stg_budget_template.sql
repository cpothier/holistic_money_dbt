{{
    config(
        materialized='view'
    )
}}

-- Extract raw budget data from Google Sheets
-- The external table configs are managed outside dbt
-- Reference the external table directly using project and dataset variables

WITH raw_budget AS (
    SELECT 
        SAFE_CAST(`string_field_0` AS STRING) AS parent_account,
        REGEXP_SUBSTR(SAFE_CAST(`string_field_1` AS STRING), '[^:]+') AS sub_account,
        REGEXP_SUBSTR(SAFE_CAST(`string_field_1` AS STRING), ":(.*)") AS child_account,
        SAFE_CAST(NULLIF(REPLACE(`string_field_9`, ',', ''), '-') AS FLOAT64) AS jan25_budget, 
        SAFE_CAST(NULLIF(REPLACE(`string_field_10`, ',', ''), '-') AS FLOAT64) AS feb25_budget, 
        SAFE_CAST(NULLIF(REPLACE(`string_field_11`, ',', ''), '-') AS FLOAT64) AS mar25_budget, 
        SAFE_CAST(NULLIF(REPLACE(`string_field_12`, ',', ''), '-') AS FLOAT64) AS apr25_budget, 
        SAFE_CAST(NULLIF(REPLACE(`string_field_13`, ',', ''), '-') AS FLOAT64) AS may25_budget, 
        SAFE_CAST(NULLIF(REPLACE(`string_field_14`, ',', ''), '-') AS FLOAT64) AS jun25_budget, 
        SAFE_CAST(NULLIF(REPLACE(`string_field_15`, ',', ''), '-') AS FLOAT64) AS jul25_budget, 
        SAFE_CAST(NULLIF(REPLACE(`string_field_16`, ',', ''), '-') AS FLOAT64) AS aug25_budget, 
        SAFE_CAST(NULLIF(REPLACE(`string_field_17`, ',', ''), '-') AS FLOAT64) AS sep25_budget,
        SAFE_CAST(NULLIF(REPLACE(`string_field_18`, ',', ''), '-') AS FLOAT64) AS oct25_budget, 
        SAFE_CAST(NULLIF(REPLACE(`string_field_19`, ',', ''), '-') AS FLOAT64) AS nov25_budget, 
        SAFE_CAST(NULLIF(REPLACE(`string_field_20`, ',', ''), '-') AS FLOAT64) AS dec25_budget, 
        SAFE_CAST(NULLIF(REPLACE(`string_field_21`, ',', ''), '-') AS FLOAT64) AS jan26_budget, 
        SAFE_CAST(NULLIF(REPLACE(`string_field_22`, ',', ''), '-') AS FLOAT64) AS feb26_budget, 
        SAFE_CAST(NULLIF(REPLACE(`string_field_23`, ',', ''), '-') AS FLOAT64) AS mar26_budget, 
        SAFE_CAST(NULLIF(REPLACE(`string_field_24`, ',', ''), '-') AS FLOAT64) AS apr26_budget, 
        SAFE_CAST(NULLIF(REPLACE(`string_field_25`, ',', ''), '-') AS FLOAT64) AS may26_budget, 
        SAFE_CAST(NULLIF(REPLACE(`string_field_26`, ',', ''), '-') AS FLOAT64) AS jun26_budget, 
        SAFE_CAST(NULLIF(REPLACE(`string_field_27`, ',', ''), '-') AS FLOAT64) AS jul26_budget, 
        SAFE_CAST(NULLIF(REPLACE(`string_field_28`, ',', ''), '-') AS FLOAT64) AS aug26_budget, 
        SAFE_CAST(NULLIF(REPLACE(`string_field_29`, ',', ''), '-') AS FLOAT64) AS sep26_budget, 
        SAFE_CAST(NULLIF(REPLACE(`string_field_30`, ',', ''), '-') AS FLOAT64) AS oct26_budget, 
        SAFE_CAST(NULLIF(REPLACE(`string_field_31`, ',', ''), '-') AS FLOAT64) AS nov26_budget, 
        SAFE_CAST(NULLIF(REPLACE(`string_field_32`, ',', ''), '-') AS FLOAT64) AS dec26_budget, 
        SAFE_CAST(NULLIF(REPLACE(`string_field_33`, ',', ''), '-') AS FLOAT64) AS jan27_budget, 
        SAFE_CAST(NULLIF(REPLACE(`string_field_34`, ',', ''), '-') AS FLOAT64) AS feb27_budget, 
        SAFE_CAST(NULLIF(REPLACE(`string_field_35`, ',', ''), '-') AS FLOAT64) AS mar27_budget, 
        SAFE_CAST(NULLIF(REPLACE(`string_field_36`, ',', ''), '-') AS FLOAT64) AS apr27_budget, 
        SAFE_CAST(NULLIF(REPLACE(`string_field_37`, ',', ''), '-') AS FLOAT64) AS may27_budget, 
        SAFE_CAST(NULLIF(REPLACE(`string_field_38`, ',', ''), '-') AS FLOAT64) AS jun27_budget, 
        SAFE_CAST(NULLIF(REPLACE(`string_field_39`, ',', ''), '-') AS FLOAT64) AS jul27_budget, 
        SAFE_CAST(NULLIF(REPLACE(`string_field_40`, ',', ''), '-') AS FLOAT64) AS aug27_budget, 
        SAFE_CAST(NULLIF(REPLACE(`string_field_41`, ',', ''), '-') AS FLOAT64) AS sep27_budget, 
        SAFE_CAST(NULLIF(REPLACE(`string_field_42`, ',', ''), '-') AS FLOAT64) AS oct27_budget, 
        SAFE_CAST(NULLIF(REPLACE(`string_field_43`, ',', ''), '-') AS FLOAT64) AS nov27_budget, 
        SAFE_CAST(NULLIF(REPLACE(`string_field_44`, ',', ''), '-') AS FLOAT64) AS dec27_budget 
    FROM `{{ env_var('DBT_BIGQUERY_PROJECT') }}.{{ env_var('DBT_CLIENT_DATASET') }}.budget_template`
)

SELECT * FROM raw_budget 