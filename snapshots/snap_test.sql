{% snapshot snap_test %}

{{
    config(
      target_schema='raw_vault',
      strategy='check',
      unique_key='id',
      check_cols=['value_1', 'value_2', 'value_3'],
      invalidate_hard_deletes=True 
    )
}}

select
  *
from lakehousenessie.staging.zz_ha_staging_table
where data_date = '20240101'

{% endsnapshot %}
