{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    incremental_strategy='merge',
    merge_update_columns=[
        'transaction_date',
        'user_id',
        'user_name',
        'user_city',
        'product_id',
        'product_name',
        'category',
        'store_id',
        'store_name',
        'region',
        'quantity',
        'amount',
        'hash_value',
        'updated_dttm'
    ]
) }}

WITH final AS (

    SELECT
        t.transaction_id,
        t.transaction_date,

        t.user_id,
        u.user_name,
        u.city AS user_city,

        t.product_id,
        p.product_name,
        p.category,

        t.store_id,
        s.store_name,
        s.region,

        t.quantity,
        t.amount

    FROM {{ ref('stg_transactions') }} t
    LEFT JOIN {{ ref('stg_users') }} u ON t.user_id = u.user_id
    LEFT JOIN {{ ref('stg_products') }} p ON t.product_id = p.product_id
    LEFT JOIN {{ ref('stg_stores') }} s ON t.store_id = s.store_id

)

SELECT
    *,

    {{ generate_hash([
        'user_id',
        'product_id',
        'store_id',
        'quantity',
        'amount'
    ]) }} AS hash_value,

    CURRENT_TIMESTAMP() AS inserted_dttm,
    CURRENT_TIMESTAMP() AS updated_dttm

FROM final
