{{
    config(
        alias="daily_sales",
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "date",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}
with cleansed_orders as (select * from {{ ref("int__cleansed_orders") }})
select
    date(order_time_jst) as date,
    sum(sales_jpy) as sales,
    count(distinct user_id) as payment_uu,
    sum(sales_jpy) / count(distinct user_id) as arppu
from cleansed_orders
{% if is_incremental() %}
    where date(order_time_jst) >= date_sub(current_date(), interval 7 day)
{% endif %}
group by 1
