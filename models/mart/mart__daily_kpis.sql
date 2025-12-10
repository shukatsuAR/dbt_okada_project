{{
    config(
        alias="daily_kpis",
        materialized="table",
        partition_by={
            "field": "date",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}
with
    daily_user_sales as (select * from {{ ref("int__daily_user_sales") }}),
    daily_registered_user_types as (
        select * from {{ ref("int__daily_registered_user_types") }}
    ),
    join_daily as (
        select *
        from daily_user_sales
        left join daily_registered_user_types using (user_id, date)
    ),
    final as (
        select
            date,
            case
                when user_type = "新規"
                then "新規"
                when user_type = "復帰" and payment_experience_flg = 1
                then "復帰課金経験"
                when user_type = "復帰" and payment_experience_flg = 0
                then "復帰無課金"
                when user_type = "既存" and payment_experience_flg = 1
                then "既存課金経験"
                when user_type = "既存" and payment_experience_flg = 0
                then "既存無課金"
            end as detail_user_type,
            count(distinct user_id) as dau,
            sum(if(user_type = "新規", 1, 0)) as new_uu,
            sum(if(d1_access_flg = 1, 1, 0)) as d1_access_uu,
            sum(if(d1_3_access_flg = 1, 1, 0)) as d1_3_access_uu,
            sum(if(d1_7_access_flg = 1, 1, 0)) as d1_7_access_uu,
            sum(if(d1_14_access_flg = 1, 1, 0)) as d1_14_access_uu,
            sum(if(sales > 0, 1, 0)) as payment_uu,
            sum(sales) as sales,
        from join_daily
        group by 1, 2
    )

select *
from final
