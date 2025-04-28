{{
    config(
        materialized='view'
    )
}}

-- First, group financial data by account hierarchy
WITH financial_data AS (
  SELECT
    txnDate,
    parent_account,
    sub_account,
    child_account,
    classification,
    account_type,
    SUM(actual) AS actual,
    SUM(budget_amount) AS budget_amount,
    -- Create a list of entry_ids for this grouping
    ARRAY_AGG(entry_id) AS entry_ids
  FROM {{ ref('materialized_pl_budget_blend') }}
  GROUP BY txnDate, parent_account, sub_account, child_account, classification, account_type
),

-- Get the most recent comment for each financial grouping
latest_comments AS (
  SELECT
    fd.txnDate,
    fd.parent_account,
    fd.sub_account,
    fd.child_account,
    fd.classification,
    fd.account_type,
    ARRAY_AGG(c.comment_text IGNORE NULLS ORDER BY c.created_at DESC LIMIT 1)[OFFSET(0)] AS comment_text,
    ARRAY_AGG(c.created_by IGNORE NULLS ORDER BY c.created_at DESC LIMIT 1)[OFFSET(0)] AS comment_by,
    ARRAY_AGG(c.created_at IGNORE NULLS ORDER BY c.created_at DESC LIMIT 1)[OFFSET(0)] AS comment_date
  FROM financial_data fd
  -- Flatten the array of entry_ids
  CROSS JOIN UNNEST(fd.entry_ids) AS entry_id
  LEFT JOIN {{ ref('financial_comments') }} c
    ON entry_id = c.entry_id
  GROUP BY fd.txnDate, fd.parent_account, fd.sub_account, fd.child_account, fd.classification, fd.account_type
)

-- Final combined view
SELECT
  fd.txnDate,
  fd.parent_account,
  fd.sub_account,
  fd.child_account,
  fd.classification,
  fd.account_type,
  fd.actual,
  fd.budget_amount,
  lc.comment_text,
  lc.comment_by,
  lc.comment_date,
  -- Include entry_ids as a string array for reference (optional)
  TO_JSON_STRING(fd.entry_ids) AS entry_ids_json
FROM financial_data fd
LEFT JOIN latest_comments lc
  ON fd.txnDate = lc.txnDate
  AND fd.parent_account = lc.parent_account
  AND fd.sub_account = lc.sub_account
  AND fd.child_account = lc.child_account
  AND fd.account_type = lc.account_type
ORDER BY fd.txnDate, fd.parent_account, fd.sub_account, fd.child_account, fd.classification, fd.account_type