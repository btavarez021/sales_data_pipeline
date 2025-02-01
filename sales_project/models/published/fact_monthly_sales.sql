WITH customer_sales as (
    select 
        o.order_id, 
        o.customer_id,
        c.customer_name ,
        o.order_date,
        o.quantity * o.price as total_sales
    FROM {{ ref("stg_orders") }} o
    inner join {{ ref("stg_customers")}} c 
    ON o.customer_id = c.customer_id
),
monthly_sales as (
    select
        customer_id, 
        date_trunc('month', order_date) as month,
        sum(total_sales) as total_sales
    FROM customer_sales
    group by 1,2
)

select * from monthly_sales