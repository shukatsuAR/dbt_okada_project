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
    daily_access as (
        select user_id, date, date_trunc(date, month) as month
        from {{ ref("int__daily_registered_user_types") }}
    ),

    monthly_first_access as (
        select user_id, month, min(date) as first_access_in_month
        from daily_access
        group by 1, 2
    ),

    user_access_history as (
        select
            monthly.user_id,
            monthly.month,
            monthly.first_access_in_month,

            lag(monthly.first_access_in_month, 1) over (
                partition by monthly.user_id order by monthly.month
            ) as previous_month_access_date
        from monthly_first_access as monthly
    )

select
    user_id,
    month,
    case
        when previous_month_access_date is null
        then '新規'
        when date_diff(first_access_in_month, previous_month_access_date, day) > 14
        then '復帰'

        else '既存'
    end as user_type

from user_access_history
