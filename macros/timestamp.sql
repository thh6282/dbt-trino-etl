{% macro trino__current_timestamp() -%}
    current_timestamp(6)
{%- endmacro %}

{% macro trino__snapshot_string_as_time(timestamp) %}
    {%- set result = "timestamp(6) '" ~ timestamp ~ "'" -%}
    {{ return(result) }}
{% endmacro %}
