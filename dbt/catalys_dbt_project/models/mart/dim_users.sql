{{ config(
    materialized='incremental',
    unique_key='user_id',
    incremental_strategy='merge',
    merge_update_columns=[
        'user_name',
        'email',
        'city',
        'marital_status',
        'signup_date',
        'hash_value',
        'updated_dttm'
    ]
) }}

SELECT
    user_id,
    user_name,
    email,
    city,
    marital_status,
    signup_date,

    {{ generate_hash([
        'user_name',
        'email',
        'city',
        'marital_status',
        'signup_date'
    ]) }} AS hash_value,

    CURRENT_TIMESTAMP() AS inserted_dttm,
    CURRENT_TIMESTAMP() AS updated_dttm

FROM {{ ref('stg_users') }}
