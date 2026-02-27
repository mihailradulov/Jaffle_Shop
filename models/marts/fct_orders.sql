--This comment is added to test CI job
select
    o.order_id,
    o.customer_id,
    sum(coalesce(p.payment_amount, 0)) as amount

from {{ ref("stg_jaffle_shop__orders") }} as o
left join
    {{ ref("stg_stripe__payments") }} as p
    on o.order_id = p.order_id
    and p.payment_status = 'success'
group by all