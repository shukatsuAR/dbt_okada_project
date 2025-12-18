{{
    config(
        alias="monthly_department_brand_sales",
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "month",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with

    cleansed_orders as (
        select *
        from {{ ref("int__cleansed_orders") }}
        {% if is_incremental() %}
            where date(order_time_jst) >= date_sub(current_date(), interval 7 day)
        {% endif %}
    ),
    monthly_registered_user_types as (
        select * from {{ ref("int__monthly_registered_user_types") }}
    ),
    orders_and_types as (
        select
            product_department,
            product_brand,
            sales_jpy,
            orders.user_id,
            date(date_trunc(orders.order_time_jst, month)) as month
        from cleansed_orders as orders
    ),

    brand_level_summary as (
        select
            user_id,
            month,
            product_department,
            product_brand,
            sales_jpy,
            count(distinct user_id) over (
                partition by month, product_department, product_brand
            ) as window_payment_uu
        from orders_and_types
    )
select
    date(month) as month,
    user_type,
    product_department as department,
    if(window_payment_uu >= 10, product_brand, "その他") as brand,
    sum(sales_jpy) as sales,
    count(distinct user_id) as payment_uu
from brand_level_summary
left join monthly_registered_user_types using (user_id, month)
group by 1, 2, 3, 4
