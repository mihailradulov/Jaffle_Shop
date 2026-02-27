select
	id as payment_id,
	orderid as order_id,
	paymentmethod as payment_method,
	status as payment_status,
	amount / 100.00 as payment_amount,
	created as create_at

from {{ source('stripe', 'payment') }}
