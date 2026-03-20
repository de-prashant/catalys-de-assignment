{{ config(
    materialized='incremental',
    unique_key='store_id',
    incremental_strategy='merge'
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
    ]) }} AS hash_value

FROM {{ source('raw', 'stores') }}
