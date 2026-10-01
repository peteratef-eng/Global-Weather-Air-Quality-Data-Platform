SELECT 
    city,
    time AT TIME ZONE 'UTC' AT TIME ZONE 'Africa/Cairo' AS weather_time,      
    time                                                AS weather_time_utc,  -- ← الوقت الأصلي من الـ API (UTC)
    temperature_2m,
    precipitation,
    relative_humidity_2m,
    wind_speed_10m,
    cloud_cover
FROM {{ source('weather_raw', 'weather_hourly') }}