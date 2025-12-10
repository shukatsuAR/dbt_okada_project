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
    events as (
        select user_id, datetime(created_at, "+9") as access_time_jst
        from {{ ref("stg__events") }}
        where user_id is not null
    ),
    orders as (
        select user_id, datetime(created_at, "+9") as access_time_jst
        from {{ ref("stg__orders") }}
    ),
    accesses as (
        select user_id, date(access_time_jst) as date
        from events
        union distinct
        select user_id, date(access_time_jst) as date
        from orders
        group by 1, 2

    ),
    daily_user_access_info as (
        select
            user_id,
            date,
            lag(date, 1) over (partition by user_id order by date) as previous_date,
            lead(date, 1) over (partition by user_id order by date) as next_date
        from accesses
    ),
    sorting_user_type as (
        select
            *,
            case
                when previous_date is null
                then "新規"
                when datetime_diff(date, previous_date, day) > 14
                then "復帰"
                else "既存"

            end as user_type,
        from daily_user_access_info

    )

select
    sorting_user_type.user_id,
    date,
    sorting_user_type.user_type,
    case
        when date_diff(sorting_user_type.next_date, sorting_user_type.date, day) = 1
        then 1
        else 0
    end as d1_access_flg,

    case
        when
            date_diff(sorting_user_type.next_date, sorting_user_type.date, day)
            between 1 and 3
        then 1
        else 0
    end as d1_3_access_flg,

    case
        when
            date_diff(sorting_user_type.next_date, sorting_user_type.date, day)
            between 1 and 7
        then 1
        else 0
    end as d1_7_access_flg,

    case
        when
            date_diff(sorting_user_type.next_date, sorting_user_type.date, day)
            between 1 and 14
        then 1
        else 0
    end as d1_14_access_flg

from sorting_user_type
