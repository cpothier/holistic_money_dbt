{{
    config(
        materialized='view'
    )
}}

WITH Unpivoted AS (
    SELECT
        parent_account,
        sub_account,
        child_account,
        -- Convert 'jan25_budget' into a proper date
        PARSE_DATE('%b%y', REPLACE(Month, '_budget', '')) AS budget_date,
        budget_amount
    FROM {{ ref('stg_budget_template') }}
    UNPIVOT(
        budget_amount FOR Month IN (jan25_budget, feb25_budget, mar25_budget, apr25_budget, 
                               may25_budget, jun25_budget, jul25_budget, aug25_budget, 
                               sep25_budget, oct25_budget, nov25_budget, dec25_budget,
                               jan26_budget, feb26_budget, mar26_budget, apr26_budget,
                               may26_budget, jun26_budget, jul26_budget, aug26_budget,
                               sep26_budget, oct26_budget, nov26_budget, dec26_budget,
                               jan27_budget, feb27_budget, mar27_budget, apr27_budget,
                               may27_budget, jun27_budget, jul27_budget, aug27_budget,
                               sep27_budget, oct27_budget, nov27_budget, dec27_budget)
    )
)

SELECT 
    (CASE WHEN parent_account.Name is NOT NULL THEN parent_account.Name ELSE sub_account.AccountType END) as parent_account,
    u.sub_account as sub_account,
    u.child_account,
    u.budget_date,
    u.budget_amount
FROM Unpivoted u
LEFT JOIN `{{ env_var('DBT_BIGQUERY_PROJECT') }}.{{ env_var('DBT_CLIENT_DATASET') }}.accounts` sub_account ON u.sub_account = sub_account.Name
LEFT JOIN `{{ env_var('DBT_BIGQUERY_PROJECT') }}.{{ env_var('DBT_CLIENT_DATASET') }}.accounts` parent_account on JSON_VALUE(sub_account.ParentRef.value) = parent_account.Id 