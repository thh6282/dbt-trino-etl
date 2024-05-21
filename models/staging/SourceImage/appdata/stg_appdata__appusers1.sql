{% if execute %}
    {% set dynamic_query = transform_data_types_iceberg("appdata", "dbo", "appusers") %}
{% endif %}

with source_data as (
    select
        {{ dynamic_query }}
    from 
)

select
    {{ dynamic_query }}
from source_data
