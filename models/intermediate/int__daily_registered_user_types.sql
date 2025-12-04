{{
    config(
        alias="daily_registered_user_types",
        materialized="table",
        partition_by={
            "field": "date",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

with
    orders as (
        select user_id, date(created_at, "+9") as orders_date
        from {{ ref("stg__orders") }}
    ),

    events as (
        select user_id, date(created_at, "+9") as date
        from {{ ref("stg__events") }}
        where user_id is not null
        group by user_id, date
    ),
    dates as (
        select
            user_id,
            date,
            lag(date, 1) over (
                partition by user_id order by date
            ) as previous_access_date,
            lead(date, 1) over (partition by user_id order by date) as next_access_date
        from events
    ),
    sorting_user_type as (
        select
            *,
            case
                when date = orders_date
                then "新規"
                when date_diff(previous_access_date, date, day) > 14
                then "復帰"
                else "既存"

            end as user_type,
        from dates
        left join orders using (user_id)
    )

select
    sorting_user_type.user_id,
    sorting_user_type.date,  -- アクセス日付
    sorting_user_type.user_type,
    case
        when
            date_diff(sorting_user_type.next_access_date, sorting_user_type.date, day)
            = 1
        then 1
        else 0
    end as d1_access_flg,

    case
        when
            date_diff(sorting_user_type.next_access_date, sorting_user_type.date, day)
            between 1 and 3
        then 1
        else 0
    end as d1_3_access_flg,

    case
        when
            date_diff(sorting_user_type.next_access_date, sorting_user_type.date, day)
            between 1 and 7
        then 1
        else 0
    end as d1_7_access_flg,

    case
        when
            date_diff(sorting_user_type.next_access_date, sorting_user_type.date, day)
            between 1 and 14
        then 1
        else 0
    end as d1_14_access_flg

from sorting_user_type
