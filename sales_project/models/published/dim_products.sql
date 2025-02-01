SELECT
    product_id,
    product_name,
    product_category,
    price,
    CURRENT_DATE AS record_loaded_date -- Add a metadata column
FROM
    {{ ref("stg_products") }}
