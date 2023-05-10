{{ 
  config(
        post_hook = ["insert into test_db.yomari.inv values(103,'hak','l','2020-3-4','2023-3-2','placed',3)"]
         )
}}

-- Define the model using a SQL query

with customers as (
    select * from {{ ref('stg_customers') }}

),

orders as (

    select * from {{ ref('stg_orders') }}
),

customer_orders as (

    select
        customer_id,

        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders,
        status

    from orders

    group by 1, status

),

final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        customer_orders.status,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders

    from customers

    left join customer_orders using (customer_id)

)

select * from final

-- Use a pre-hook to insert a record into a table before the model is executed


-- Use a post-hook to update a table after the model is executed
