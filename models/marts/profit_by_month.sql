{{
    config(
        materialized='view'
    )
}}

SELECT 
    txnDate,
    SUM(CASE WHEN parent_account = 'Income' THEN actual ELSE 0 END) - 
        SUM(CASE WHEN parent_account = 'Cost of Goods Sold' THEN actual ELSE 0 END) as gross_profit_actual,
    
    SUM(CASE WHEN parent_account = 'Income' THEN budget_amount ELSE 0 END) - 
        SUM(CASE WHEN parent_account = 'Cost of Goods Sold' THEN budget_amount ELSE 0 END) as gross_profit_budget,
    
    SUM(CASE WHEN parent_account = 'Income' THEN actual ELSE 0 END) - 
        SUM(CASE WHEN parent_account = 'Income' THEN 0 ELSE actual END) as net_profit_actual,
    
    SUM(CASE WHEN parent_account = 'Income' THEN budget_amount ELSE 0 END) - 
        SUM(CASE WHEN parent_account = 'Income' THEN 0 ELSE budget_amount END) as net_profit_budget
FROM {{ ref('pl_budget_with_comments') }}
GROUP BY txnDate
ORDER BY txnDate 