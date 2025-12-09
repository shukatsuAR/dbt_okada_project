{{
    config(
        alias="daily_user_sales",
        materialized="table",
        partition_by={
            "field": "date",
            "data_type": "date",
            "granularity": "day",
        },
        cluster_by=["user_id", "date"],
    )
}}
with
    cleansed_orders as (select * from {{ ref("int__cleansed_orders") }}),
    orders_daily as (
        -- 1. 注文データを日次・ユーザーID単位で集計し、当日の売上と注文があった日付を準備
        select date(order_time_jst) as date, user_id, sum(sales_jpy) as daily_sales
        from cleansed_orders
        group by 1, 2
    ),

    all_users_dates as (
        -- 2. 全ユーザーの全活動期間の日付マトリクスを作成
        -- これにより、売上が0の日も行として保持される
        select all_sales_date.date, all_user_id.user_id
        from
            (
                -- 全ての注文日付を取得
                select distinct date(order_time_jst) as date from cleansed_orders
            ) as all_sales_date
        cross join
            (
                -- 全てのユーザーIDを取得
                select distinct user_id from cleansed_orders
            ) as all_user_id
    ),

    base_table as (
        -- 3. 日付マトリクスに日次売上を結合し、NULLを0埋め
        select
            all_users_dates.date,
            all_users_dates.user_id,
            if(
                orders_daily.daily_sales is not null, orders_daily.daily_sales, 0
            ) as daily_sales_amount
        from all_users_dates
        left join
            orders_daily
            on all_users_dates.date = orders_daily.date
            and all_users_dates.user_id = orders_daily.user_id
    ),

    -- **キー指標計算**
    calculated_metrics as (
        select
            date,
            user_id,
            daily_sales_amount,

            -- 4. 当日売上（当日を含む2日間の合計）
            -- 売上は当日のみ
            sum(daily_sales_amount) over (
                partition by user_id
                order by date asc
                rows between current row and current row
            ) as sales,

            -- 5. 過去30日間売上（当日を含まない30日前〜1日前の合計）
            -- 現在の行（当日）を含めず、過去30日間（2日前〜31日前）の合計
            sum(daily_sales_amount) over (
                partition by user_id
                order by date asc
                rows between 31 preceding and 1 preceding
            ) as past_d30_sales,

            -- 6. 過去累計売上（ユーザー登録日〜1日前までの合計）
            -- 現在の行（当日）を含めず、全期間の合計
            sum(daily_sales_amount) over (
                partition by user_id
                order by date asc
                rows between unbounded preceding and 1 preceding
            ) as past_all_sales
        from base_table
    )

-- **セグメント・フラグ付与**
select
    cm.user_id,
    cm.date,
    cm.sales,
    if(cm.past_d30_sales is null, 0, cm.past_d30_sales) as past_d30_sales,
    if(cm.past_all_sales is null, 0, cm.past_all_sales) as past_all_sales,

    -- 7. 30日間課金セグメントの分類 (past_d30_salesに基づき当日を除外)
    case
        when cm.past_d30_sales >= 50001
        then 'a_50,001円~'
        when cm.past_d30_sales >= 30001
        then 'b_30,001円~50,000円'
        when cm.past_d30_sales >= 10001
        then 'c_10,001円~30,000円'
        when cm.past_d30_sales >= 5001
        then 'd_5,001円~10,000円'
        when cm.past_d30_sales >= 3001
        then 'e_3,001円~5,000円'
        when cm.past_d30_sales >= 1001
        then 'f_1,001円~3,000円'
        when cm.past_d30_sales >= 1
        then 'g_1円~1,000円'
        else 'h_0円'
    end as past_d30_payment_segment,

    -- 8. 課金経験フラグ (過去累計売上 (past_all_sales) が1円以上なら1、そうでなければ0)
    case
        when if(cm.past_all_sales is null, 0, cm.past_all_sales) > 0 then 1 else 0
    end as payment_experience_flg

from calculated_metrics as cm
-- past_all_sales (累計売上) は1日前のデータを使用するため、
-- ユーザーが注文した最初の日付以降のデータのみを対象とする
where
    cm.date >= (
        select min(date(order_time_jst))
        from cleansed_orders as orders_min
        where orders_min.user_id = cm.user_id
    )
