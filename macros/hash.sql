{% macro hash_column_list(columns_list) %}

{% set hex_columns = [] %}

{% set len = columns_list | length %}

{% for column in columns_list %}

    {% if loop.index != len  %}
        {% set column_string = "'App_data' || COALESCE(CAST(" ~ column ~ " AS varchar), '') ||" %}
        {% do hex_columns.append(column_string) %}
    {% else %}
        {% set column_string = "'App_data' || COALESCE(CAST(" ~ column ~ " AS varchar), '')" %}
        {% do hex_columns.append(column_string) %}
    {% endif %}

{% endfor %}

{% set str = hex_columns | join('') %}

to_hex(md5(to_utf8({{str}})))

{% endmacro %}

---------------
{% macro hash_column(column) %}

{%- set column_string = "'App_data' || COALESCE(CAST(" ~ column ~ " AS varchar), '')" -%}
to_hex(md5(to_utf8({{column_string}})))

{% endmacro %}