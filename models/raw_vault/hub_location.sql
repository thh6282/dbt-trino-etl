{{
    config(
        materialized='incremental',
        schema = 'raw_vault',
        views_enabled=False
    )
}}    

select
    {{hash_column('locationcode')}} as location_hash_Key,
    load_date,
    locationcode as location_id
from staging.stg_appdata__wh_location   
where 1=1    
