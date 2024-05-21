{{ config(
        materialized='incremental',
        schema = 'staging',
        views_enabled=False,
) }}

{% if execute %}
        {% set dynamic_query = transform_data_types_iceberg("appdata", "dbo", "WH_Location_User") %}
{% endif %}

SELECT 
    {{ dynamic_query }},
    '{{ var("source_name") }}' AS source_name,
    '{{ var("load_date") }}' AS load_date,
    CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS ppn_tm
FROM appdata.dbo.WH_Location_User