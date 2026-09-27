SELECT city, day, COUNT(*) AS n
FROM {{ ref('mart_weather_daily') }}
GROUP BY city, day
HAVING COUNT(*) > 1