select
    orderid as order_id,
    status,
    PAYMENTMETHOD as payment_method,
    {{ cents_to_dollars('amount', 4) }} as amount,
    max(created) as payment_finalized_date

from {{ source('stripe', 'payment') }}
{{ dbt_utils.group_by(n=3) }}
