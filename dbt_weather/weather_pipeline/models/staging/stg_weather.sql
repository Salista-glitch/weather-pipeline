

with source as (
    select * from {{ source('weather_data', 'daily_forecasts') }}
),

cleaned as (
    select
        date,
        city,
        round(max_temp, 2) as max_temp,
        round(min_temp, 2) as min_temp,
        round(precipitation, 2) as precipitation,
        round(temp_range, 2) as temp_range,
        condition,
        current_timestamp() as processed_at
    from source
)

select * from cleaned