select 
    DISTINCT  
            customer_id, 
            INITCAP(name) as customer_name,
            lower(email) as customer_email,
            REGEXP_REPLACE(phone, '^[0-9]', '') as customer_phone,
            address,
            _file,
            _line,
            _modified,
            _fivetran_synced
FROM {{ source("fivetran", "customers") }}
WHERE customer_id is not null