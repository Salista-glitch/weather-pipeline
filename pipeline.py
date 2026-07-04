import requests
import pandas as pd
import psycopg2
import logging
from prefect import task, flow, get_run_logger

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()
    ],
    force=True
)
logger = logging.getLogger(__name__)

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
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data["daily"]
    except requests.exceptions.Timeout:
        raise Exception("API request timed out after 10 seconds")
    except requests.exceptions.HTTPError as e:
        raise Exception(f"API returned an error: {e}")
    except Exception as e:
        raise Exception(f"Extraction failed: {e}")


@task
def validate(df):
    logger = get_run_logger()
    errors = []

    # Check we have rows
    if len(df) == 0:
        errors.append("DataFrame is empty — no data returned from API")

    # Check expected columns exist
    expected_columns = ["date", "max_temp", "min_temp", "precipitation"]
    for col in expected_columns:
        if col not in df.columns:
            errors.append(f"Missing expected column: {col}")

    # Check for nulls in critical columns
    for col in ["date", "max_temp", "min_temp"]:
        if df[col].isnull().any():
            errors.append(f"Null values found in column: {col}")

    # Check temperature makes physical sense
    if (df["max_temp"] < df["min_temp"]).any():
        errors.append("max_temp is less than min_temp in one or more rows")

    if errors:
        for error in errors:
            logger.error(f"Data quality check failed: {error}")
        raise Exception(f"Validation failed with {len(errors)} error(s)")

    logger.info(f"Data validation passed — {len(df)} rows, all checks clean")
    return df

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

    logger = get_run_logger()

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
    logger.info(f"Loaded {len(df)} rows into weather_data")


@flow(name="weather-etl")
def run_pipeline():

    logger = get_run_logger()

    logger.info("Extracting data from Open-Meteo API")
    daily = extract()
    logger.info("Transforming dataset")
    df = transform(daily)
    logger.info("Validating data quality")
    df = validate(df)
    logger.info("Loading data into Postgres")
    load(df)
    logger.info("Pipeline complete")

if __name__ == "__main__":
    run_pipeline()