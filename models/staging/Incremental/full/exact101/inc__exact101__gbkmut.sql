{% if execute %}
    {% set dynamic_query = transform_data_types_iceberg("mssql", "dbo", "gbkmut") %}
{% endif %}

{% if is_incremental() %}

with source_data as (
    select
        id
    from mssql.dbo.gbkmut    
    where datum >= (select max(datum) from lakehousenessie.staging.stg_exact101__gbkmut)
)
{% endif %}

select  
    {{ dynamic_query }},
    '{{ var("source_name") }}' AS source_name,
    '{{ var("load_date") }}' AS load_date,
    CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS ppn_tm
from source_data
limit 10000