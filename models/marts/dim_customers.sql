with customers as (

    select * from {{ ref('stg_jaffle_shop__customers') }}

),

orders as (

    select * from {{ ref('stg_jaffle_shop__orders') }}

),

customer_orders as (

    select
        o.customer_id,
        min(o.order_date) as first_order_date,
        max(o.order_date) as most_recent_order_date,
        count(o.order_id) as number_of_orders,
        sum(coalesce(p.payment_amount, 0)) as lifetime_value

    from orders as o

    left join {{ ref('stg_stripe__payments') }} as p
        on o.order_id = p.order_id and p.payment_status = 'success'

    group by 1

),

final as (

    select
        c.customer_id,
        c.first_name,
        c.last_name,
        o.first_order_date,
        o.most_recent_order_date,
        o.number_of_orders,
        coalesce(o.lifetime_value, 0.00) as lifetime_value

    from customers as c

    left join customer_orders as o
        on c.customer_id = o.customer_id

)

select * from final