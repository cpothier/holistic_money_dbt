{{
    config(
        materialized='view'
    )
}}

SELECT
  t.entry_id,
  t.ordering_id,
  t.txnDate,
  t.parent_account,
  t.sub_account,
  t.child_account,
  t.actual,
  t.budget_amount,
  c.comment_text,
  c.created_by AS comment_by,
  c.created_at AS comment_date
FROM {{ ref('materialized_pl_budget_blend') }} t
LEFT JOIN {{ ref('financial_comments') }} c
  ON t.entry_id = c.entry_id
ORDER BY t.ordering_id, t.txnDate, t.parent_account, t.sub_account, t.child_account 