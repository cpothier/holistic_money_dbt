{% macro source_exists(source_name, table_name) %}
    {% set sources = graph.sources.values() %}
    {% for source in sources %}
        {% if source.source_name == source_name and source.name == table_name %}
            {% do return(true) %}
        {% endif %}
    {% endfor %}
    {% do return(false) %}
{% endmacro %} 