select 
    customer_id, 
     customer_name,
    customer_email,
    customer_phone,
    address,
    CURRENT_DATE as record_loaded_date
FROM {{ ref("stg_customers") }}