{{ config(
    materialized='incremental',
    unique_key='store_id',
    incremental_strategy='merge',
    merge_update_columns=[
        'store_name',
        'city',
        'state',
        'region',
        'opening_date',
        'hash_value',
        'updated_dttm'
    ]
) }}

SELECT
    store_id,
    store_name,
    city,
    state,
    region,
    opening_date,

    {{ generate_hash([
        'store_name',
        'city',
        'state',
        'region',
        'opening_date'
    ]) }} AS hash_value,

    CURRENT_TIMESTAMP() AS inserted_dttm,
    CURRENT_TIMESTAMP() AS updated_dttm

FROM {{ ref('stg_stores') }}
