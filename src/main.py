import requests
import json
import datetime
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.postgresql import insert

load_dotenv()

expected_units = {
    "temperature_2m": "°C",
    "precipitation": "mm",
    "relative_humidity_2m": "%",
    "wind_speed_10m": "km/h"
}

db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")

connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
engine = create_engine(connection_string)


def fetch_weather_data(url):
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException as e:
        print(f"Failed {e}")
        return None

    if response.status_code == 200:
        print(f"{response.status_code} API Connection Successful")
        data = response.json()
        return data
    else:
        print(f"{response.status_code} API Connection Faild")
        return None



def validation_units(data, expected_units):
    for param, unit in expected_units.items():
        actual_unit = data["hourly_units"][param]
        if actual_unit == unit:
            print(f"agreed: {param}: {unit}")
        else:
            print(f"faild: {param}: {unit}")


def save_raw_data(data):
    now = datetime.datetime.now()
    now_str = now.strftime("%Y-%m-%d_%H-%M-%S")
    file_name = f"weather_raw_{now_str}.json"
    with open(f"data/raw/{file_name}", "w") as f:
        json.dump(data, f)


def convert_to_dataframe(data):
    df = pd.DataFrame(data['hourly'])
    df['time'] = pd.to_datetime(df['time'])
    return df


def load_to_postgres(df, table_name, engine):
    records = df.to_dict(orient="records")
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)

    with engine.begin() as connection:
        stmt = insert(table).values(records)
        stmt = stmt.on_conflict_do_nothing(index_elements=['time'])
        result = connection.execute(stmt)

    print(f"Inserted {result.rowcount} new rows, skipped duplicates")

if __name__ == "__main__":
    url = "https://api.open-meteo.com/v1/forecast?latitude=30.0444&longitude=31.2357&hourly=temperature_2m,precipitation,relative_humidity_2m,wind_speed_10m&forecast_days=7"
    data = fetch_weather_data(url)
    if data is not None:
        validation_units(data, expected_units)
        save_raw_data(data)
        df = convert_to_dataframe(data)
        load_to_postgres(df, "weather_hourly", engine)
    else:
        print("faild to bring data and the pipeline stoped")