SELECT 
    DATE(weather_time) AS day,
    ROUND(AVG(temperature_2m):: numeric, 2) AS avg_temp,
    ROUND(MAX(temperature_2m):: numeric, 2) AS max_temp,
    ROUND(MIN(temperature_2m):: numeric, 2) AS min_temp,
    ROUND(SUM(precipitation):: numeric, 2) AS total_precipitation,
    ROUND(AVG(relative_humidity_2m):: numeric, 2) AS avg_humidity,
    ROUND(MAX(relative_humidity_2m):: numeric, 2) AS max_humidity,
    ROUND(MIN(relative_humidity_2m):: numeric, 2) AS min_humidity,
    ROUND(AVG(wind_speed_10m):: numeric, 2) AS avg_wind_speed,
    ROUND(MAX(wind_speed_10m):: numeric, 2) AS max_wind_speed,
    ROUND(MIN(wind_speed_10m):: numeric, 2) AS min_wind_speed
FROM    
    {{ ref('stg_weather_hourly')}}
GROUP BY 
    DATE(weather_time)