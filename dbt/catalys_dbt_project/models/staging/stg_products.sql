{{ config(
    materialized='incremental',
    unique_key='product_id',
    incremental_strategy='merge'
) }}

SELECT
    product_id,
    product_name,
    category,
    sub_category,
    brand,
    price,

    {{ generate_hash([
        'product_name',
        'category',
        'sub_category',
        'brand',
        'price'
    ]) }} AS hash_value

FROM {{ source('raw', 'products') }}
