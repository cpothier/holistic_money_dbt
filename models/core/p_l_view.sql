{{
    config(
        materialized='view'
    )
}}

-- Deposits
WITH deposits_data AS (
    SELECT
        CAST(txnDate AS DATE) AS txnDate,
        account.Classification AS classification,
        account.Name AS account_name,
        (CASE WHEN account.SubAccount IS TRUE THEN parent_account1.Name ELSE account.AccountType END) AS parent_account_name,
        (CASE WHEN parent_account1.SubAccount IS TRUE THEN parent_account2.Name ELSE parent_account1.AccountType END) AS parent_account_name2,
        SUM(CAST(JSON_VALUE(line.Amount) AS FLOAT64)) AS amount
    FROM `{{ env_var('DBT_BIGQUERY_PROJECT') }}.{{ env_var('DBT_CLIENT_DATASET') }}.deposits` AS deposits,
    UNNEST(JSON_EXTRACT_ARRAY(Line)) AS line 
    JOIN `{{ env_var('DBT_BIGQUERY_PROJECT') }}.{{ env_var('DBT_CLIENT_DATASET') }}.accounts` AS account ON account.id = JSON_VALUE(line.DepositLineDetail.AccountRef.value)
    LEFT JOIN `{{ env_var('DBT_BIGQUERY_PROJECT') }}.{{ env_var('DBT_CLIENT_DATASET') }}.accounts` AS parent_account1 ON parent_account1.id = JSON_VALUE(account.ParentRef.value)
    LEFT JOIN `{{ env_var('DBT_BIGQUERY_PROJECT') }}.{{ env_var('DBT_CLIENT_DATASET') }}.accounts` AS parent_account2 ON parent_account2.id = JSON_VALUE(parent_account1.ParentRef.value)
    GROUP BY txnDate, classification, account_name, parent_account_name, parent_account_name2
),

-- Transformed deposits with standardized column names
transformed_deposits AS (
    SELECT
        txnDate,
        classification,
        (CASE WHEN parent_account_name2 IS NULL THEN parent_account_name ELSE parent_account_name2 END) AS parent_account,
        (CASE WHEN parent_account_name2 IS NULL THEN account_name ELSE parent_account_name END) AS sub_account,
        (CASE WHEN parent_account_name2 IS NULL THEN CAST(NULL AS STRING) ELSE account_name END) AS child_account,
        amount
    FROM deposits_data
),

-- Purchases
purchases_data AS (
    SELECT
        CAST(txnDate AS DATE) AS txnDate,
        account.Classification AS classification,
        account.Name AS account_name,
        parent_account1.Name AS parent_account_name,
        parent_account2.Name AS parent_account_name2,
        SUM((CASE WHEN account.AccountType = "Income" 
                THEN CAST(JSON_VALUE(line.Amount) AS FLOAT64)*-1
                WHEN purchases.credit THEN CAST(JSON_VALUE(line.Amount) AS FLOAT64)*-1 
                ELSE CAST(JSON_VALUE(line.Amount) AS FLOAT64) END)) AS amount
    FROM `{{ env_var('DBT_BIGQUERY_PROJECT') }}.{{ env_var('DBT_CLIENT_DATASET') }}.purchases` AS purchases,
    UNNEST(JSON_EXTRACT_ARRAY(purchases.Line)) AS line
    JOIN `{{ env_var('DBT_BIGQUERY_PROJECT') }}.{{ env_var('DBT_CLIENT_DATASET') }}.accounts` AS account ON account.id = JSON_VALUE(line.AccountBasedExpenseLineDetail.AccountRef.value)
    LEFT JOIN `{{ env_var('DBT_BIGQUERY_PROJECT') }}.{{ env_var('DBT_CLIENT_DATASET') }}.accounts` AS parent_account1 ON parent_account1.id = JSON_VALUE(account.ParentRef.value)
    LEFT JOIN `{{ env_var('DBT_BIGQUERY_PROJECT') }}.{{ env_var('DBT_CLIENT_DATASET') }}.accounts` AS parent_account2 ON parent_account2.id = JSON_VALUE(parent_account1.ParentRef.value)
    GROUP BY txnDate, classification, account_name, parent_account_name, parent_account_name2
),

-- Transformed purchases with standardized column names
transformed_purchases AS (
    SELECT
        txnDate,
        classification,
        (CASE WHEN parent_account_name2 IS NULL THEN parent_account_name ELSE parent_account_name2 END) AS parent_account,
        (CASE WHEN parent_account_name2 IS NULL THEN account_name ELSE parent_account_name END) AS sub_account,
        (CASE WHEN parent_account_name2 IS NULL THEN CAST(NULL AS STRING) ELSE account_name END) AS child_account,
        amount
    FROM purchases_data
),

-- Journal Entries
journal_entries_data AS (
    SELECT
        CAST(txnDate AS DATE) AS txnDate,
        account.Classification AS classification,
        account.Name AS account_name,
        (CASE WHEN account.SubAccount IS TRUE THEN parent_account1.Name ELSE account.AccountType END) AS parent_account_name,
        CAST(parent_account2.Name AS STRING) AS parent_account_name2,
        SUM((CASE WHEN JSON_VALUE(line.JournalEntryLineDetail.PostingType) = 'Credit' 
                AND account.AccountType != "Income" 
                THEN CAST(JSON_VALUE(line.Amount) AS FLOAT64)*-1 
                ELSE CAST(JSON_VALUE(line.Amount) AS FLOAT64) END)) AS amount
    FROM `{{ env_var('DBT_BIGQUERY_PROJECT') }}.{{ env_var('DBT_CLIENT_DATASET') }}.journal_entries` AS je,
    UNNEST(JSON_EXTRACT_ARRAY(Line)) AS line 
    JOIN `{{ env_var('DBT_BIGQUERY_PROJECT') }}.{{ env_var('DBT_CLIENT_DATASET') }}.accounts` AS account ON account.id = JSON_VALUE(line.JournalEntryLineDetail.AccountRef.value)
    LEFT JOIN `{{ env_var('DBT_BIGQUERY_PROJECT') }}.{{ env_var('DBT_CLIENT_DATASET') }}.accounts` AS parent_account1 ON parent_account1.id = JSON_VALUE(account.ParentRef.value)
    LEFT JOIN `{{ env_var('DBT_BIGQUERY_PROJECT') }}.{{ env_var('DBT_CLIENT_DATASET') }}.accounts` AS parent_account2 ON parent_account2.id = JSON_VALUE(parent_account1.ParentRef.value)
    WHERE account.AccountType IN ("Income","Expense", "Cost of Goods Sold")
    GROUP BY txnDate, classification, account_name, parent_account_name, parent_account_name2
),

-- Transformed journal entries with standardized column names
transformed_journal_entries AS (
    SELECT
        txnDate,
        classification,
        (CASE WHEN parent_account_name2 IS NULL THEN parent_account_name ELSE parent_account_name2 END) AS parent_account,
        (CASE WHEN parent_account_name2 IS NULL THEN account_name ELSE parent_account_name END) AS sub_account,
        (CASE WHEN parent_account_name2 IS NULL THEN CAST(NULL AS STRING) ELSE account_name END) AS child_account,
        amount
    FROM journal_entries_data
),

-- Payments
payments_data AS (
    SELECT
        CAST(payments.txnDate AS DATE) AS txnDate,
        account.Classification AS classification,
        account.Name AS account_name,
        (CASE WHEN account.SubAccount IS TRUE THEN parent_account1.Name ELSE account.AccountType END) AS parent_account_name,
        (CASE WHEN parent_account1.SubAccount IS TRUE THEN parent_account2.Name ELSE parent_account1.AccountType END) AS parent_account_name2,
        SUM(CAST(JSON_VALUE(line.Amount) AS FLOAT64) * (COALESCE(payments.amount,0) / invoices.TotalAmt)) AS amount
    FROM `{{ env_var('DBT_BIGQUERY_PROJECT') }}.{{ env_var('DBT_CLIENT_DATASET') }}.invoices` AS invoices,
    UNNEST(JSON_EXTRACT_ARRAY(invoices.Line)) AS line 
    JOIN `{{ env_var('DBT_BIGQUERY_PROJECT') }}.{{ env_var('DBT_CLIENT_DATASET') }}.items` AS items ON items.Id = JSON_VALUE(line.SalesItemLineDetail.ItemRef.value)
    JOIN `{{ env_var('DBT_BIGQUERY_PROJECT') }}.{{ env_var('DBT_CLIENT_DATASET') }}.accounts` AS account ON account.id = JSON_VALUE(items.IncomeAccountRef.value)
    LEFT JOIN `{{ env_var('DBT_BIGQUERY_PROJECT') }}.{{ env_var('DBT_CLIENT_DATASET') }}.accounts` AS parent_account1 ON parent_account1.id = JSON_VALUE(account.ParentRef.value)
    LEFT JOIN `{{ env_var('DBT_BIGQUERY_PROJECT') }}.{{ env_var('DBT_CLIENT_DATASET') }}.accounts` AS parent_account2 ON parent_account2.id = JSON_VALUE(parent_account1.ParentRef.value)
    JOIN (
        SELECT 
            payments.txnDate, 
            line, 
            CAST(JSON_VALUE(payments.Line[0], '$.Amount') AS FLOAT64) AS amount, 
            JSON_VALUE(payments.Line[0], '$.LinkedTxn[0].TxnId') AS invoice_id
        FROM `{{ env_var('DBT_BIGQUERY_PROJECT') }}.{{ env_var('DBT_CLIENT_DATASET') }}.payments` payments
    ) payments ON payments.invoice_id = invoices.Id
    GROUP BY txnDate, classification, account_name, parent_account_name, parent_account_name2
),

-- Transformed payments with standardized column names
transformed_payments AS (
    SELECT
        txnDate,
        classification,
        (CASE WHEN parent_account_name2 IS NULL THEN parent_account_name ELSE parent_account_name2 END) AS parent_account,
        (CASE WHEN parent_account_name2 IS NULL THEN account_name ELSE parent_account_name END) AS sub_account,
        (CASE WHEN parent_account_name2 IS NULL THEN CAST(NULL AS STRING) ELSE account_name END) AS child_account,
        amount
    FROM payments_data
),

-- Bill Payments
bill_payments_data AS (
    SELECT
        CAST(payments.txnDate AS DATE) AS txnDate,
        account.Classification AS classification,
        account.Name AS account_name,
        (CASE WHEN account.SubAccount IS TRUE THEN parent_account1.Name ELSE account.AccountType END) AS parent_account_name,
        CAST(parent_account2.Name AS STRING) AS parent_account_name2,
        SUM(CAST(JSON_VALUE(line.Amount) AS FLOAT64) * (COALESCE(payments.payment_amount,0) / bills.TotalAmt)) AS amount
    FROM `{{ env_var('DBT_BIGQUERY_PROJECT') }}.{{ env_var('DBT_CLIENT_DATASET') }}.bills` AS bills,
    UNNEST(JSON_EXTRACT_ARRAY(bills.Line)) AS line 
    JOIN `{{ env_var('DBT_BIGQUERY_PROJECT') }}.{{ env_var('DBT_CLIENT_DATASET') }}.items` AS items ON items.Id = JSON_VALUE(line.AccountBasedExpenseLineDetail.ItemRef.value)
    JOIN `{{ env_var('DBT_BIGQUERY_PROJECT') }}.{{ env_var('DBT_CLIENT_DATASET') }}.accounts` AS account ON account.id = JSON_VALUE(items.ExpenseAccountRef.value)
    LEFT JOIN `{{ env_var('DBT_BIGQUERY_PROJECT') }}.{{ env_var('DBT_CLIENT_DATASET') }}.accounts` AS parent_account1 ON parent_account1.id = JSON_VALUE(account.ParentRef.value)
    LEFT JOIN `{{ env_var('DBT_BIGQUERY_PROJECT') }}.{{ env_var('DBT_CLIENT_DATASET') }}.accounts` AS parent_account2 ON parent_account2.id = JSON_VALUE(parent_account1.ParentRef.value)
    JOIN (
        SELECT 
            payments.txnDate, 
            line, 
            CAST(JSON_VALUE(payments.Line[0], '$.Amount') AS FLOAT64) AS payment_amount, 
            JSON_VALUE(payments.Line[0], '$.LinkedTxn[0].TxnId') AS bill_id
        FROM `{{ env_var('DBT_BIGQUERY_PROJECT') }}.{{ env_var('DBT_CLIENT_DATASET') }}.bill_payments` payments
    ) payments ON payments.bill_id = bills.Id
    GROUP BY txnDate, classification, account_name, parent_account_name, parent_account_name2
),

-- Transformed bill payments with standardized column names
transformed_bill_payments AS (
    SELECT
        txnDate,
        classification,
        (CASE WHEN parent_account_name2 IS NULL THEN parent_account_name ELSE parent_account_name2 END) AS parent_account,
        (CASE WHEN parent_account_name2 IS NULL THEN account_name ELSE parent_account_name END) AS sub_account,
        (CASE WHEN parent_account_name2 IS NULL THEN CAST(NULL AS STRING) ELSE account_name END) AS child_account,
        amount
    FROM bill_payments_data
),

-- Sales Receipts
sales_receipts_data AS (
    SELECT
        CAST(txnDate AS DATE) AS txnDate,
        account.Classification AS classification,
        account.Name AS account_name,
        (CASE WHEN account.SubAccount IS TRUE THEN parent_account1.Name ELSE account.AccountType END) AS parent_account_name,
        (CASE WHEN parent_account1.SubAccount IS TRUE THEN parent_account2.Name ELSE parent_account1.AccountType END) AS parent_account_name2,
        SUM(CAST(JSON_VALUE(line.Amount) AS FLOAT64)) AS amount
    FROM `{{ env_var('DBT_BIGQUERY_PROJECT') }}.{{ env_var('DBT_CLIENT_DATASET') }}.sales_receipts` AS sr,
    UNNEST(JSON_EXTRACT_ARRAY(sr.Line)) AS line 
    JOIN `{{ env_var('DBT_BIGQUERY_PROJECT') }}.{{ env_var('DBT_CLIENT_DATASET') }}.items` AS items ON items.Id = JSON_VALUE(line.SalesItemLineDetail.ItemRef.value)
    JOIN `{{ env_var('DBT_BIGQUERY_PROJECT') }}.{{ env_var('DBT_CLIENT_DATASET') }}.accounts` AS account ON account.id = JSON_VALUE(items.IncomeAccountRef.value)
    LEFT JOIN `{{ env_var('DBT_BIGQUERY_PROJECT') }}.{{ env_var('DBT_CLIENT_DATASET') }}.accounts` AS parent_account1 ON parent_account1.id = JSON_VALUE(account.ParentRef.value)
    LEFT JOIN `{{ env_var('DBT_BIGQUERY_PROJECT') }}.{{ env_var('DBT_CLIENT_DATASET') }}.accounts` AS parent_account2 ON parent_account2.id = JSON_VALUE(parent_account1.ParentRef.value)
    GROUP BY txnDate, classification, account_name, parent_account_name, parent_account_name2
),

-- Transformed sales receipts with standardized column names
transformed_sales_receipts AS (
    SELECT
        txnDate,
        classification,
        (CASE WHEN parent_account_name2 IS NULL THEN parent_account_name ELSE parent_account_name2 END) AS parent_account,
        (CASE WHEN parent_account_name2 IS NULL THEN account_name ELSE parent_account_name END) AS sub_account,
        (CASE WHEN parent_account_name2 IS NULL THEN CAST(NULL AS STRING) ELSE account_name END) AS child_account,
        amount
    FROM sales_receipts_data
)

-- Combine all transformed data
SELECT 
    txnDate,
    classification,
    parent_account,
    sub_account,
    child_account,
    SUM(amount) as amount
FROM (
    SELECT * FROM transformed_deposits
    UNION ALL
    SELECT * FROM transformed_purchases
    UNION ALL
    SELECT * FROM transformed_journal_entries
    UNION ALL
    SELECT * FROM transformed_payments
    UNION ALL
    SELECT * FROM transformed_bill_payments
    UNION ALL
    SELECT * FROM transformed_sales_receipts
)
GROUP BY txnDate, classification, parent_account, sub_account, child_account
ORDER BY txnDate DESC 