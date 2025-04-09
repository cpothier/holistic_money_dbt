{% macro check_source(source_name, table_name) %}

{% set sql %}
SELECT COUNT(*) as count
FROM `{{ env_var('DBT_BIGQUERY_PROJECT') }}.{{ env_var('DBT_CLIENT_DATASET') }}.__TABLES__`
WHERE table_id = '{{ table_name }}'
{% endset %}

{% set result = run_query(sql) %}
{% set exists = result[0][0] > 0 %}

{% do return(exists) %}

{% endmacro %} 