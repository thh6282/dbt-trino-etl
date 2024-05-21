{{ config(
    materialized='incremental',
    schema='staging',
    views_enabled=False,
) }}

{% set columns_list = ['id', 'value_1', 'value_2', 'value_3']
%}

with source as (
    select 
        id,
        value_1,
        value_2,
        value_3,
        {{hash_column_list(columns_list)}} as hash_diff
    from staging.zz_ha_staging_table
    where data_date = '20240102'
),

staging as (
    select 
        id,
        value_1,
        value_2,
        value_3,
        {{hash_column_list(columns_list)}} as hash_diff
    from staging.zz_ha_staging_table
    where data_date = '20240101'
)

select  
    source.id,
    source.value_1,
    source.value_2,
    source.value_3,
    '{{ var("source_name") }}' AS source_name,
    '{{ var("load_date") }}' AS load_date,
    CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS ppn_tm
from source
left join staging
on source.id = staging.id
where source.hash_diff != staging.hash_diff or staging.id is null
