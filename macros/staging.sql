{% macro transform_data_types_iceberg(catalog_name, schema_name, table_name) %}

-- Mapping Trino data types to Iceberg data types
{% set type_mappings = {
    'INT': 'INTEGER',
    'LONG': 'BIGINT',
    'FLOAT': 'REAL',
    'TIME': 'TIME(6)',
    'TIMESTAMP': 'TIMESTAMP(6)',
    'TIMESTAMP(3)': 'TIMESTAMP(6)',
    'TIMESTAMPZ': 'TIMESTAMP(6) WITH TIME ZONE',
    'STRING': 'VARCHAR',
    'BINARY': 'VARBINARY',
    'VARBINARY': 'VARBINARY',
    'CHAR': 'VARCHAR',
    'SMALLINT': 'INTEGER'
} %}

-- Start building the transformed columns list
{% set transformed_columns = [] %}
{% set full_table_name = catalog_name ~ '.' ~ schema_name ~ '.' ~ table_name %}
{% set columns = adapter.get_columns_in_relation((full_table_name)) %}

-- Iterate through each column and apply the appropriate type transformation
{% for column in columns %}

    {% if column.dtype.upper() == 'VARBINARY' %}
        -- Handle VARBINARY columns appropriately

        {% set transformed_column = column.name %}

    {% else %}
        {% set transformed_type = type_mappings.get(column.data_type.upper(), column.data_type) %}

        {% set transformed_column = "CAST(" ~ column.name ~ " AS " ~ transformed_type ~ ") AS " ~ column.name %}
    {% endif %}
    {% do transformed_columns.append(transformed_column) %}
{% endfor %}

    {% set dynamic_query = transformed_columns | join(", ") %}
    {{ return(dynamic_query) }}

{% endmacro %}