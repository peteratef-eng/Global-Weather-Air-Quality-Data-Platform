import requests
import json
import datetime
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.postgresql import insert
from pathlib import Path

load_dotenv()

CITIES = [
    {"name": "alexandria", "lat": 31.2058, "lon": 29.9245},
    {"name": "cairo",      "lat": 30.0444, "lon": 31.2357},
]

expected_units = {
    "temperature_2m": "°C",
    "precipitation": "mm",
    "relative_humidity_2m": "%",
    "wind_speed_10m": "km/h",
    "cloud_cover": "%"
}

def run_pipeline_for_city(city_name, lat, lon, engine):
    url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&hourly=temperature_2m,precipitation,relative_humidity_2m,wind_speed_10m,cloud_cover&forecast_days=14"

    try:
        response = requests.get(url)
    except requests.exceptions.RequestException as e:
        print(f"connection failed: {e}")
        return None
    if response.status_code == 200:
        print(f"connection succesfully : {response.status_code}")
        data = response.json()
    else:
        print(f"connection failed : {response.status_code}")
        return
    for param, unit in expected_units.items():
        actual_unit = data["hourly_units"][param]
        if actual_unit == unit:
            print(f"agreed: {param}: {unit}")
        else:
            print(f"faild: {param}: {unit}")

    now = datetime.datetime.now()
    now_str = now.strftime("%Y-%m-%d_%H-%M-%S")
    file_name = f"weather_hourly_{city_name}_{now_str}.json"
    Path("data/raw").mkdir(parents=True, exist_ok=True)

    with open(f"data/raw/{file_name}", "w") as f:
        json.dump(data, f)

    df = pd.DataFrame(data['hourly'])
    df['city'] = city_name
    df['time'] = pd.to_datetime(df['time'])
        

    records = df.to_dict(orient="records")
    metadata = MetaData()
    table = Table("weather_hourly", metadata, autoload_with=engine)
    
    with engine.begin() as connection:
        stmt = insert(table).values(records)
        stmt = stmt.on_conflict_do_nothing(index_elements=['city','time'])
        result = connection.execute(stmt)
    
    print(f"Inserted {result.rowcount} new rows, skipped duplicates")


db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")

connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
engine = create_engine(connection_string)

for city in CITIES:
    run_pipeline_for_city(city["name"], city['lat'], city['lon'], engine)
