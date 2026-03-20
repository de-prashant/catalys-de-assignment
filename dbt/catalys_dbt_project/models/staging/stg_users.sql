{{ config(
    materialized='incremental',
    unique_key='user_id',
    incremental_strategy='merge'
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
    ]) }} AS hash_value

FROM {{ source('raw', 'users') }}
