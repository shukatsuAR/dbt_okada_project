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
    accesses as (
        select user_id, date(created_at, "+9") as access_time_jst
        from {{ ref("stg__events") }}
        union all
        select user_id, date(created_at, "+9") as access_time_jst
        from {{ ref("stg__orders") }}
    ),
    daily_user_access_info as (
        select
            user_id,
            access_time_jst,
            lag(access_time_jst, 1) over (
                partition by user_id order by access_time_jst
            ) as previous_access_date,
            lead(access_time_jst, 1) over (
                partition by user_id order by access_time_jst
            ) as next_access_date
        from accesses
    ),
    sorting_user_type as (
        select
            *,
            case
                when previous_access_date is null
                then "新規"
                when date_diff(access_time_jst, previous_access_date, day) > 14
                then "復帰"
                else "既存"

            end as user_type,
        from daily_user_access_info

    )

select
    sorting_user_type.user_id,
    date(sorting_user_type.access_time_jst) as date,
    sorting_user_type.user_type,
    case
        when
            date_diff(
                sorting_user_type.next_access_date,
                sorting_user_type.access_time_jst,
                day
            )
            = 1
        then 1
        else 0
    end as d1_access_flg,

    case
        when
            date_diff(
                sorting_user_type.next_access_date,
                sorting_user_type.access_time_jst,
                day
            )
            between 1 and 3
        then 1
        else 0
    end as d1_3_access_flg,

    case
        when
            date_diff(
                sorting_user_type.next_access_date,
                sorting_user_type.access_time_jst,
                day
            )
            between 1 and 7
        then 1
        else 0
    end as d1_7_access_flg,

    case
        when
            date_diff(
                sorting_user_type.next_access_date,
                sorting_user_type.access_time_jst,
                day
            )
            between 1 and 14
        then 1
        else 0
    end as d1_14_access_flg

from sorting_user_type
