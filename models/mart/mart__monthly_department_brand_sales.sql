{{
    config(
        alias="monthly_department_brand_sales",
        materialized="table",
        partition_by={
            "field": "month",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with
    join_orders_and_types as (
        select
            monthly.month,
            monthly.user_type,
            orders.product_department,
            orders.product_brand,
            sum(orders.sales_jpy) as sales,
            count(distinct monthly.user_id) as payment_uu,
        from {{ ref("int__cleansed_orders") }} as orders
        left join
            {{ ref("int__monthly_registered_user_types") }} as monthly
            on date_trunc(orders.order_time_jst, month) = monthly.month
        where monthly.user_id is not null
        group by 1, 2, 3, 4
    )
select
    month,
    user_type,
    product_department as department,
    if(payment_uu >= 10, product_brand, "その他") as brand,
    sales,
    payment_uu
from join_orders_and_types
