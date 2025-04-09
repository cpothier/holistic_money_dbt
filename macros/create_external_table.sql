{% macro create_external_budget_table(budget_sheet_url, sheet_range) %}

{% set sql %}
CREATE OR REPLACE EXTERNAL TABLE `{{ env_var('DBT_BIGQUERY_PROJECT') }}.{{ env_var('DBT_CLIENT_DATASET') }}.budget_template`
OPTIONS (
  format = 'GOOGLE_SHEETS',
  uris = ['{{ budget_sheet_url }}'],
  sheet_range = '{{ sheet_range }}',
  skip_leading_rows = 1
);
{% endset %}

{% do run_query(sql) %}

{% endmacro %} 