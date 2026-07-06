

with base as (
    select * from {{ ref('stg_weather') }}
),

summary as (
    select
        city,
        count(*) as forecast_days,
        round(avg(max_temp), 2) as avg_max_temp,
        round(avg(min_temp), 2) as avg_min_temp,
        round(max(max_temp), 2) as highest_temp,
        round(min(min_temp), 2) as lowest_temp,
        round(avg(temp_range), 2) as avg_daily_range,
        round(sum(precipitation), 2) as total_precipitation,
        countif(condition = 'hot') as hot_days,
        countif(condition = 'mild') as mild_days,
        countif(condition = 'cold') as cold_days
    from base
    group by city
)

select * from summary