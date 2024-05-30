{{ config(
    materialized='incremental',
    schema = 'staging',
    unique_key='hash_diff',
    views_enabled=False,
) }}        

{% if execute %}
    {% set dynamic_query = transform_data_types_iceberg("appdata", "dbo", "appusers") %}
{% endif %}
select  
    {{ dynamic_query }},
    '{{ var("source_name") }}' AS source_name,
    '{{ var("load_date") }}' AS load_date,
    CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS ppn_tm
from appdata.dbo.appuser
where synmodified = '{{var(etl_date)}}'
