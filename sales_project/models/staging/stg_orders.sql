select 
    DISTINCT order_id, 
            customer_id, 
            product_id,
            cast(order_date as DATE) as order_date,
            quantity,
            price,
            _file,
            _line,
            _modified,
            _fivetran_synced
FROM {{ source("fivetran", "orders") }}
WHERE order_id is not null