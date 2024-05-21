
{{
    config(
        materialized='incremental',
        schema = 'raw_vault',
        unique_key=['link_hash_key'] ,
        incremental_strategy='merge',
        views_enabled='false'
    )
}}    

select
    to_hex(md5(to_utf8(
    'App_Data' || '_' || COALESCE(cast(DeliveryNumber as varchar), '') || 
    'App_Data' || '_' || COALESCE(cast(DriverName as varchar), '') || 
    'App_Data' || '_' || COALESCE(cast(VehicleId as varchar), '') || 
    'App_Data' || '_' || COALESCE(cast(InvoiceNumber as varchar), '') || 
    'App_Data' || '_' || COALESCE(cast(ordernr as varchar), '') || 
    'App_Data' || '_' || COALESCE(cast(pakbon_nr as varchar), '') || 
    'App_Data' || '_' || COALESCE(cast(kstplcode as varchar), '') || 
    'App_Data' || '_' || COALESCE(cast(debnr as varchar), '') || 
    'App_Data' || '_' || COALESCE(cast(magcode as varchar), '')
    ))) as link_hash_key,
    to_date('20240515', 'yyyymmdd') as load_date,
    to_date('99990101', 'yyyymmdd') as load_end_date,
    'App_Data' || '_' || 'MPLDeliveryManager' as record_source,
    cast(SendVatInvoice as varchar) as send_vat_invoice_flag,
    cast(SendPhotoInvoice as varchar) as send_photo_invoice_flag,
    cast(SendPO as varchar) as send_po_flag,
    cast(SendDeliveryNotes as varchar) as send_deliverynotes_flag,
    cast(ReturnPhotoInvoice as varchar) as return_photo_invoice_flag,
    cast(ReturnBusStatation as varchar)return_bus_station_flag,
    cast(ReturnReceiptInvoice as varchar)return_receipt_invoice_flag,
    cast(ReturnDeliveryNotes as varchar) as return_delivery_notes_flag
from staging.stg_appdata__mpldeliverymanager
where 1=1