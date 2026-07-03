import requests
import pandas as pd
import psycopg2
from prefect import task, flow


@task
def extract():
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": -26.2041,
        "longitude": 28.0473,
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
        "timezone": "Africa/Johannesburg",
        "forecast_days": 7
    }
    response = requests.get(url, params=params)
    data = response.json()
    return data["daily"]

@task
def classify_temp(temp):
    if temp > 25:
        return "hot"
    elif temp > 20:
        return "mild"
    else:
        return "cold"


@task
def transform(daily):
    df = pd.DataFrame({
        "date": daily["time"],
        "max_temp": daily["temperature_2m_max"],
        "min_temp": daily["temperature_2m_min"],
        "precipitation": daily["precipitation_sum"]
    })
    df["date"] = pd.to_datetime(df["date"])
    df["temp_range"] = df["max_temp"] - df["min_temp"]
    df["condition"] = df["max_temp"].apply(classify_temp)
    df["city"] = "Johannesburg"
    return df


@task
def load(df):
    conn = psycopg2.connect(
        host="localhost",
        database="weather_pipeline",
        user="pipeline_user",
        password="pipeline123"
    )
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            date DATE,
            max_temp FLOAT,
            min_temp FLOAT,
            precipitation FLOAT,
            temp_range FLOAT,
            condition VARCHAR(20),
            city VARCHAR(50)
        )
    """)

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO weather_data
            (date, max_temp, min_temp, precipitation, temp_range, condition, city)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date, city) DO UPDATE SET
            max_temp = EXCLUDED.max_temp,
            min_temp = EXCLUDED.min_temp,
            precipitation = EXCLUDED.precipitation,
            temp_range = EXCLUDED.temp_range,
            condition = EXCLUDED.condition
        """, (
            row["date"], row["max_temp"], row["min_temp"],
            row["precipitation"], row["temp_range"],
            row["condition"], row["city"]
        ))

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Loaded {len(df)} rows into weather_data")


@flow(name="weather-etl", log_prints=True)
def run_pipeline():
    print("Extracting...")
    daily = extract()
    print("Transforming...")
    df = transform(daily)
    print("Loading...")
    load(df)
    print("Pipeline complete.")

if __name__ == "__main__":
    run_pipeline()