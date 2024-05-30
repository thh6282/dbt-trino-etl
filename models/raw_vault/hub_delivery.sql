{{
    config(
        materialized='incremental',
        schema = 'raw_vault',
        unique_key=['delivery_hash_key'] ,
        incremental_strategy='merge',
        views_enabled=False,
    )
}}    
with sqlmain as (
select
    to_hex(md5(to_utf8('App_Data' || '_' || COALESCE(cast(DeliveryNumber as varchar), '')))) as delivery_hash_key,
    load_date as load_date,
    'App_Data' || '_' || 'MPLDeliveryManager' as record_source,
    DeliveryNumber as delivery_id
from staging.stg_appdata__mpldeliverymanager
)
select * from sqlmain a

{% if is_cremental()%}
    {where not EXISTS (select 1 from {{ this }} b where a.delivery_hash_key= b.delivery_hash_key)}
{% endif %}
