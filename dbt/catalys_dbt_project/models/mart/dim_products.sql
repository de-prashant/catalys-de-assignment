{{ config(
    materialized='incremental',
    unique_key='product_id',
    incremental_strategy='merge',
    merge_update_columns=[
        'product_name',
        'category',
        'sub_category',
        'brand',
        'price',
        'hash_value',
        'updated_dttm'
    ]
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
    ]) }} AS hash_value,

    CURRENT_TIMESTAMP() AS inserted_dttm,
    CURRENT_TIMESTAMP() AS updated_dttm

FROM {{ ref('stg_products') }}
