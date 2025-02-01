select 
    DISTINCT 
            product_id,
            initcap(name) as product_name,
            initcap(category) as product_category,
            price,
            _file,
            _line,
            _modified,
            _fivetran_synced
FROM {{ source("fivetran", "products") }}
WHERE price > 0