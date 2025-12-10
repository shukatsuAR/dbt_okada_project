{{
    config(
        alias="daily_user_sales",
        materialized="table",
        partition_by={
            "field": "date",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}
with
    cleansed_orders as (select * from {{ ref("int__cleansed_orders") }}),
    daily_registered_user_types as (
        select * from {{ ref("int__daily_registered_user_types") }}
    ),
    cleansed_orders_add_date as (
        select *, date(order_time_jst) as date from cleansed_orders
    ),
    orders_daily as (
        -- 1. 注文データを日次・ユーザーID単位で集計し、当日の売上と注文があった日付を準備
        select user_id, date, sum(coalesce(sales_jpy, 0)) as sales
        from daily_registered_user_types
        left join cleansed_orders_add_date using (user_id, date)
        group by 1, 2
        having user_id is not null
    ),
    calc_sales as (
        select
            *,
            -- 30日前から前日の売上
            sum(sales) over (
                partition by user_id
                order by unix_date(date)
                range between 30 preceding and 1 preceding
            ) as past_d30_sales,

            -- 初回購入から前日までの売上
            sum(sales) over (
                partition by user_id
                order by unix_date(date)
                range between unbounded preceding and 1 preceding
            ) as past_all_sales

        from orders_daily
    ),
    final as (
        select
            user_id,
            date,
            coalesce(sales, 0) as sales,
            coalesce(past_d30_sales, 0) as past_d30_sales,
            coalesce(past_all_sales, 0) as past_all_sales,
            -- 30日間課金セグメントの分類 (past_d30_salesに基づき当日を除外)
            case
                when past_d30_sales >= 50001
                then 'a_50,001円~'
                when past_d30_sales >= 30001
                then 'b_30,001円~50,000円'
                when past_d30_sales >= 10001
                then 'c_10,001円~30,000円'
                when past_d30_sales >= 5001
                then 'd_5,001円~10,000円'
                when past_d30_sales >= 3001
                then 'e_3,001円~5,000円'
                when past_d30_sales >= 1001
                then 'f_1,001円~3,000円'
                when past_d30_sales >= 1
                then 'g_1円~1,000円'
                else 'h_0円'
            end as past_d30_payment_segment,
            case
                when coalesce(past_all_sales, 0) > 0 then 1 else 0
            end as payment_experience_flg
        from calc_sales
    )
select *
from final
