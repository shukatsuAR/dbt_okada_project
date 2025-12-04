{{
    config(
        alias="monthly_registered_user_types",
        materialized="table",
        partition_by={
            "field": "month",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with
    daily_registered_user_types as (
        select *, date_trunc(date, month) as month
        from {{ ref("int__daily_registered_user_types") }}
    ),

    monthly_first_access as (
        select user_id, month, user_type, min(date) as first_access_in_month
        from daily_registered_user_types
        group by 1, 2, 3
    ),

    user_access_history as (
        select
            monthly.user_id,
            monthly.month,
            monthly.first_access_in_month,
            monthly.user_type,
            lag(monthly.first_access_in_month, 1) over (
                partition by monthly.user_id order by monthly.month
            ) as previous_month_access_date
        from monthly_first_access as monthly
    )

select
    user_id,
    month,
    case
        when user_type = '新規' then '新規' when user_type = '復帰' then '復帰' else '既存'
    end as user_type

from user_access_history
