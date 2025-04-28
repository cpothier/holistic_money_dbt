-- depends_on: {{ ref('financial_comments') }}
{{
    config(
        materialized='table',
        unique_key=['txnDate', 'parent_account', 'sub_account', 'child_account', 'classification']
    )
}}

{% set first_run = not adapter.get_relation(this.database, this.schema, this.name) %}

{% if first_run %}
-- First run, create a new table with generated UUIDs
WITH actuals_data AS (
    SELECT 
        pl.txnDate, 
        pl.parent_account, 
        pl.sub_account, 
        pl.child_account, 
        pl.classification,
        pl.account_type,
        SUM(pl.amount) as actual, 
        0 as budget_amount  -- Zero for budget amount in actuals data
    FROM {{ ref('p_l_view') }} pl
    GROUP BY pl.txnDate, pl.parent_account, pl.sub_account, pl.child_account, pl.classification, pl.account_type
),

budget_data AS (
    SELECT
        bt.budget_date as txnDate,
        bt.parent_account,
        bt.sub_account,
        bt.child_account,
        bt.classification,
        bt.account_type,
        0 as actual,  -- Zero for actual amount in budget data
        SUM(bt.budget_amount) as budget_amount
    FROM {{ ref('budget_transformed') }} bt
    GROUP BY bt.budget_date, bt.parent_account, bt.sub_account, bt.child_account, bt.classification, bt.account_type
),

-- Combine both datasets with UNION
combined_data AS (
    SELECT * FROM actuals_data
    UNION ALL
    SELECT * FROM budget_data
)

-- Aggregate to handle any potential duplicates
SELECT
    GENERATE_UUID() as entry_id,
    txnDate, 
    parent_account, 
    sub_account, 
    child_account, 
    classification,
    account_type,
    SUM(actual) as actual, 
    SUM(budget_amount) as budget_amount,
    CURRENT_TIMESTAMP() as last_refreshed
FROM combined_data
GROUP BY txnDate, parent_account, sub_account, child_account, classification, account_type

{% else %}
-- Subsequent runs: Merge to preserve entry_ids for comment relationships
WITH actuals_data AS (
    SELECT 
        pl.txnDate, 
        pl.parent_account, 
        pl.sub_account, 
        pl.child_account, 
        pl.classification,
        pl.account_type,
        SUM(pl.amount) as actual, 
        0 as budget_amount  -- Zero for budget amount in actuals data
    FROM {{ ref('p_l_view') }} pl
    GROUP BY pl.txnDate, pl.parent_account, pl.sub_account, pl.child_account, pl.classification, pl.account_type
),

budget_data AS (
    SELECT
        bt.budget_date as txnDate,
        bt.parent_account,
        bt.sub_account,
        bt.child_account,
        bt.classification,
        bt.account_type,
        0 as actual,  -- Zero for actual amount in budget data
        SUM(bt.budget_amount) as budget_amount
    FROM {{ ref('budget_transformed') }} bt
    GROUP BY bt.budget_date, bt.parent_account, bt.sub_account, bt.child_account, bt.classification, bt.account_type
),

-- Combine both datasets with UNION
new_data AS (
    SELECT * FROM actuals_data
    UNION ALL
    SELECT * FROM budget_data
),

-- Aggregate to handle any potential duplicates
aggregated_data AS (
    SELECT
        txnDate, 
        parent_account, 
        sub_account, 
        child_account, 
        classification,
        account_type,
        SUM(actual) as actual, 
        SUM(budget_amount) as budget_amount
    FROM new_data
    GROUP BY txnDate, parent_account, sub_account, child_account, classification, account_type
),

-- Use a merge pattern to preserve entry_ids
merged_data AS (
    SELECT
        t.entry_id,
        s.txnDate,
        s.parent_account,
        s.sub_account,
        s.child_account,
        s.classification,
        s.account_type,
        s.actual,
        s.budget_amount,
        CURRENT_TIMESTAMP() as last_refreshed
    FROM aggregated_data s
    LEFT JOIN {{ this }} t
        ON t.txnDate = s.txnDate
        AND t.parent_account = s.parent_account
        AND t.sub_account = s.sub_account
        AND (t.child_account = s.child_account OR (t.child_account IS NULL AND s.child_account IS NULL))
    
    UNION ALL
    
    -- Include records from the existing table that aren't in the new data but have comments
    SELECT
        t.entry_id,
        t.txnDate,
        t.parent_account,
        t.sub_account,
        t.child_account,
        t.classification,
        t.account_type,
        0 as actual, -- Zero out actuals for historical entries
        0 as budget_amount,
        t.last_refreshed
    FROM {{ this }} t
    LEFT JOIN aggregated_data s
        ON t.txnDate = s.txnDate
        AND t.parent_account = s.parent_account
        AND t.sub_account = s.sub_account
        AND (t.child_account = s.child_account OR (t.child_account IS NULL AND s.child_account IS NULL))
    -- Only keep entries that have comments attached
    JOIN {{ ref('financial_comments') }} c ON t.entry_id = c.entry_id
)

-- Generate UUID for new records only
SELECT
    COALESCE(entry_id, GENERATE_UUID()) as entry_id,
    txnDate,
    parent_account,
    sub_account,
    child_account,
    classification,
    account_type,
    actual,
    budget_amount,
    last_refreshed
FROM merged_data

{% endif %} 