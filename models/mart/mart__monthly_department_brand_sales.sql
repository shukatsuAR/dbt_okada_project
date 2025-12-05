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
    orders_and_types as (
        select
            monthly.month,
            user_type,
            product_department,
            product_brand,
            sales_jpy,
            orders.user_id,
        from {{ ref("int__cleansed_orders") }} as orders
        left join
            {{ ref("int__monthly_registered_user_types") }} as monthly
            on date_trunc(orders.order_time_jst, month) = monthly.month
    ),

    brand_level_summary as (
        select
            month,
            user_type,
            product_department,
            product_brand,
            sales_jpy,
            count(distinct user_id) over (
                partition by month, product_department, product_brand
            ) as window_payment_uu
        from orders_and_types
    )
select
    month,
    user_type,
    product_department as department,
    if(window_payment_uu >= 10, product_brand, "その他") as brand,
    sum(sales_jpy) as sales,
    max(window_payment_uu) as payment_uu
from brand_level_summary
group by 1, 2, 3, 4
