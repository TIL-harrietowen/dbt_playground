with

    -- imports
    orders as (select * from {{ ref("stg_jaffel_shop__orders") }}),

    customers as (select * from {{ ref("stg_jaffel_shop__customers") }}),

    payments as (

        select * from {{ ref("stg_stripe__payments") }} where status <> 'fail'
    ),

    -- logical
    paid_orders as

    (
        select
            orders.order_id,
            orders.customer_id,
            orders.order_date as order_placed_at,
            orders.status as order_status,
            payments.amount as total_amount_paid,
            payments.payment_finalized_date,
            customers.first_name as customer_first_name,
            customers.last_name as customer_last_name

        from orders

        left join payments on orders.order_id = payments.order_id

        left join customers on orders.customer_id = customers.customer_id
    ),

    customer_orders as

    (
        select
            customers.customer_id,
            min(orders.order_date) as first_order_date,
            max(orders.order_date) as most_recent_order_date,
            count(orders.order_id) as number_of_orders,

        from customers

        left join orders on orders.customer_id = customers.customer_id
        group by 1
    ),

    final as (
        select
            paid_orders.*,

            -- sales transaction sequence
            row_number() over (order by paid_orders.order_id) as transaction_seq,

            -- customer sales sequence
            row_number() over (
                partition by paid_orders.customer_id order by paid_orders.order_id
            ) as customer_sales_seq,

            -- new vs returning customer
            case
                when customer_orders.first_order_date = paid_orders.order_placed_at then 'new' else 'return'
            end as nvsr,

            -- customer lifetime value
            sum(total_amount_paid) over (
                partition by paid_orders.customer_id order by paid_orders.order_placed_at
            ) as customer_lifetime_value,

            -- first day of sale
            customer_orders.first_order_date as fdos

        from paid_orders

        left join customer_orders on paid_orders.customer_id = customer_orders.customer_id

    )

select *
from final

order by order_id
