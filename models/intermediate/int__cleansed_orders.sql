{{
    config(
        alias="cleansed_orders",
        materialized="table",
        partition_by={
            "field": "order_time_jst",
            "data_type": "datetime",
            "granularity": "day",
        },
        cluster_by=["user_id"],
    )
}}

with
    orders as (select * from {{ ref("stg__orders") }}),

    order_items as (select * from {{ ref("stg__order_items") }}),

    products as (select * from {{ ref("stg__products") }})

select
    orders.order_id,
    orders.user_id,
    datetime(orders.created_at, "+9") as order_time_jst,
    order_items.product_id,
    inventory_item_id,
    sale_price * 150 as sales_jpy,
    products.category as product_category,
    products.name as product_name,
    products.brand as product_brand,
    products.department as product_department,
from orders
left join order_items using (order_id)
left join products on order_items.product_id = products.id
where
    order_items.status not in ("Cancelled", "Returned")
    or orders.status not in ("Cancelled", "Returned")
