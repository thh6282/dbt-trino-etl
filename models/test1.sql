{{
    config(
        materialized='incremental',
        unique_key=['link_hash_key'],
        incremental_strategy='merge',
        merge_update_columns=['load_end_date'],
        views_enabled=False
    )
}}
{% set link_hash_key = hash_column_list(['id', 'value_1', 'value_2', 'value_3']) %}

{% if not is_incremental() %}

SELECT
    {{ link_hash_key }} AS link_hash_key,
    value_1,
    value_2,
    value_3,
    CURRENT_TIMESTAMP AS ppn_tm,
    TO_DATE('20240517', 'yyyymmdd') AS load_date,
    TO_DATE('99990101', 'yyyymmdd') AS load_end_date
FROM lakehousenessie.staging.zz_ha_staging_table;
{% else %}
{% set source_table = "lakehousenessie.staging.zz_ha_staging_table" %}
{% set target_table = "lakehousenessie.datavault.test" %}
{% set far_future_time = "'9999-01-01'" %}
{% set current_timestamp = "CURRENT_TIMESTAMP" %}
-- Get source data
WITH source_data AS (
    SELECT
        id,
        value_1,
        value_2,
        value_3,
        CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS Load_date,
        to_date('9999-12-31', 'yyyy-MM-dd') AS load_end_date
        {{ link_hash_key }} AS link_hash_key
    FROM {{ source_table }}
),

-- Get target data
target_data AS (
    SELECT
        id,
        value_1,
        value_2,
        value_3,
        CAST(CURRENT_TIMESTAMP AS timestamp(6)) AS Load_date,
        to_date('9999-12-31', 'yyyy-MM-dd') AS load_end_date
        link_hash_key
    FROM {{ target_table }}
    WHERE load_end_date = {{ far_future_time }}
),

-- Find records to update
to_update AS (
    SELECT
        t.link_hash_key,
        {{ current_timestamp }} AS load_end_date
    FROM target_data t
    JOIN source_data s ON t.link_hash_key = s.link_hash_key
    WHERE t.link_hash_key <> s.link_hash_key
),

-- Find records to insert
to_insert AS (
    SELECT
        id,
        value_1,
        value_2,
        value_3,
        link_hash_key,
        {{ far_future_time }} AS load_end_date,
        {{ current_timestamp }} AS load_date
    FROM source_data s
    LEFT JOIN target_data t ON s.link_hash_key = t.link_hash_key
    WHERE t.link_hash_key IS NULL
    OR t.link_hash_key <> s.link_hash_key
),

-- Perform updates
UPDATE {{ target_table }}
SET load_end_date = {{ current_timestamp }}
WHERE link_hash_key IN (SELECT link_hash_key FROM to_update);

-- Perform inserts
INSERT INTO {{ target_table }} (
    id,
    value_1,
    value_2,
    value_3,
    link_hash_key,
    load_date,
    load_end_date
),
SELECT
    id,
    value_1,
    value_2,
    value_3,
    link_hash_key,
    load_date,
    load_end_date
FROM to_insert;
{% endif %}