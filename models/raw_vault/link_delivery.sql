{{
    config(
        materialized='incremental',
        schema = 'raw_vault',
        unique_key=['link_hash_key'] ,
        incremental_strategy='merge',
        view_enable='false'
    )
}}    

select
    to_hex(md5(to_utf8(
    'App_Data' || '_' || COALESCE(cast(DeliveryNumber as varchar), '') || 'App_Data' || '_' || COALESCE(cast(DriverName as varchar), '') || 
    'App_Data' || '_' || COALESCE(cast(VehicleId as varchar), '') || 
    'App_Data' || '_' || COALESCE(cast(InvoiceNumber as varchar), '') || 
    'App_Data' || '_' || COALESCE(cast(ordernr as varchar), '') || 
    'App_Data' || '_' || COALESCE(cast(pakbon_nr as varchar), '') || 
    'App_Data' || '_' || COALESCE(cast(kstplcode as varchar), '') || 
    'App_Data' || '_' || COALESCE(cast(debnr as varchar), '') || 
    'App_Data' || '_' || COALESCE(cast(magcode as varchar), '') 
    ))) as link_hash_key,
    load_date,
    'App_Data' || '_' || 'MPLDeliveryManager' as record_source,
    to_hex(md5(to_utf8('App_Data' || '_' || COALESCE(cast(DeliveryNumber as varchar), '')))) as delivery_hash_key,
    to_hex(md5(to_utf8('App_Data' || '_' || COALESCE(cast(DriverName as varchar), '')))) as user_hash_key,
    to_hex(md5(to_utf8('App_Data' || '_' || COALESCE(cast(VehicleId as varchar), '')))) as vehicle_hash_key,
    to_hex(md5(to_utf8('App_Data' || '_' || COALESCE(cast(InvoiceNumber as varchar), '')))) as invoice_hash_key,
    to_hex(md5(to_utf8('App_Data' || '_' || COALESCE(cast(ordernr as varchar), '')))) as order_hash_key,
    to_hex(md5(to_utf8('App_Data' || '_' || COALESCE(cast(pakbon_nr as varchar), '')))) as deliverynote_hash_key,
    to_hex(md5(to_utf8('App_Data' || '_' || COALESCE(cast(kstplcode as varchar), '')))) as cost_center_hash_key,
    to_hex(md5(to_utf8('App_Data' || '_' || COALESCE(cast(debnr as varchar), '')))) as debtor_hash_key,
    to_hex(md5(to_utf8('App_Data' || '_' || COALESCE(cast(magcode as varchar), '')))) as warehouse_hash_key
from staging.stg_appdata__mpldeliverymanager
where 1=1

