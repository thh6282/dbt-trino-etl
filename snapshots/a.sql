{% snapshot a %}

{{
    config(
      target_schema='raw_vault',
      strategy='check',
      unique_key='hash_id',
      check_cols=['value_1', 'value_2', 'value_3'],
      invalidate_hard_deletes=True 
    )
}}

select
  *,
  {{ hash_column('id') }} as hash_id
from lakehousenessie.staging.zz_ha_staging_table
where data_date = '20240101'

{% endsnapshot %}