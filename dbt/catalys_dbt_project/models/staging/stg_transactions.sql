{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    incremental_strategy='merge'
) }}

SELECT
    transaction_id,
    user_id,
    product_id,
    store_id,
    payment_method,
    quantity,
    unit_price,
    discount,
    amount,
    transaction_date,

    {{ generate_hash([
        'user_id',
        'product_id',
        'store_id',
        'payment_method',
        'quantity',
        'unit_price',
        'discount',
        'amount',
        'transaction_date'
    ]) }} AS hash_value

FROM {{ source('raw', 'transactions') }}
