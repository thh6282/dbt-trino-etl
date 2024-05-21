
{{ config(
    materialized='incremental',
    schema = 'staging',
    views_enabled=False,
) }}        

{% if execute %}
    {% set dynamic_query = transform_data_types_iceberg("appdata", "dbo", "smpromoprogramdetailresult") %}
{% endif %}
select  
    {{ dynamic_query }},
    '{{ var("source_name") }}' AS source_name,
    '{{ var("load_date") }}' AS load_date,
    CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS ppn_tm
from appdata.dbo.smpromoprogramdetailresult