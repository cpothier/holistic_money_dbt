{{
    config(
        materialized='view'
    )
}}

SELECT 
    txnDate,
    SUM(CASE WHEN account_type = 'Income' THEN actual ELSE 0 END) - 
        SUM(CASE WHEN account_type = 'Cost of Goods Sold' THEN actual ELSE 0 END) as gross_profit_actual,
    
    SUM(CASE WHEN account_type = 'Income' THEN budget_amount ELSE 0 END) - 
        SUM(CASE WHEN account_type = 'Cost of Goods Sold' THEN budget_amount ELSE 0 END) as gross_profit_budget,
    
    SUM(CASE WHEN account_type = 'Income' THEN actual ELSE 0 END) - 
        SUM(CASE WHEN account_type IN ('Cost of Goods Sold', 'Expense') THEN actual else 0 END) as net_profit_actual,
    
    SUM(CASE WHEN account_type = 'Income' THEN budget_amount ELSE 0 END) - 
        SUM(CASE WHEN account_type IN ('Cost of Goods Sold', 'Expense') THEN budget_amount else 0 END) as net_profit_budget,

    SUM(CASE WHEN account_type = 'Income' THEN actual ELSE 0 END) - 
        SUM(CASE WHEN account_type = 'Income' THEN 0 ELSE actual END) as net_cash_actual,
    
    SUM(CASE WHEN account_type = 'Income' THEN budget_amount ELSE 0 END) - 
        SUM(CASE WHEN account_type = 'Income' THEN 0 ELSE budget_amount END) as net_cash_budget
FROM {{ ref('pl_budget_with_comments') }}
GROUP BY txnDate
ORDER BY txnDate 