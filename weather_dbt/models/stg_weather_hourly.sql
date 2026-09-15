SELECT 
    time AS weather_time,
    temperature_2m,
    precipitation,
    relative_humidity_2m,
    wind_speed_10m
FROM {{ source('weather_raw', 'weather_hourly')}}