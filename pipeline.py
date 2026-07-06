import requests
import pandas as pd
import json
import logging
from datetime import datetime, timezone
from google.cloud import bigquery, storage
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

PROJECT_ID = "white-dispatch-495014-u6"
BUCKET_NAME = f"{PROJECT_ID}-weather-raw"
DATASET_ID = "weather_data"
TABLE_ID = "daily_forecasts"

@task
def extract():
    logger = get_run_logger()
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
        logger.info("Data extracted from Open-Meteo API")
        return data
    except requests.exceptions.Timeout:
        raise Exception("API request timed out after 10 seconds")
    except requests.exceptions.HTTPError as e:
        raise Exception(f"API returned an error: {e}")
    except Exception as e:
        raise Exception(f"Extraction failed: {e}")

@task
def store_raw(data):
    logger = get_run_logger()
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    blob_name = f"johannesburg/{timestamp}.json"
    blob = bucket.blob(blob_name)
    blob.upload_from_string(
        json.dumps(data),
        content_type="application/json"
    )
    logger.info(f"Raw data stored to gs://{BUCKET_NAME}/{blob_name}")
    return data["daily"]

def classify_temp(temp):
    if temp > 25:
        return "hot"
    elif temp > 20:
        return "mild"
    else:
        return "cold"

@task
def transform(daily):
    logger = get_run_logger()
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
    logger.info(f"Transformed {len(df)} rows")
    return df

@task
def validate(df):
    logger = get_run_logger()
    errors = []

    if len(df) == 0:
        errors.append("DataFrame is empty")

    expected_columns = ["date", "max_temp", "min_temp", "precipitation"]
    for col in expected_columns:
        if col not in df.columns:
            errors.append(f"Missing column: {col}")

    for col in ["date", "max_temp", "min_temp"]:
        if df[col].isnull().any():
            errors.append(f"Null values in column: {col}")

    if (df["max_temp"] < df["min_temp"]).any():
        errors.append("max_temp less than min_temp in one or more rows")

    if errors:
        for error in errors:
            logger.error(f"Validation failed: {error}")
        raise Exception(f"Validation failed with {len(errors)} error(s)")

    logger.info(f"Validation passed — {len(df)} rows, all checks clean")
    return df

@task
def load_to_bigquery(df):
    logger = get_run_logger()
    bq_client = bigquery.Client()
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("max_temp", "FLOAT"),
            bigquery.SchemaField("min_temp", "FLOAT"),
            bigquery.SchemaField("precipitation", "FLOAT"),
            bigquery.SchemaField("temp_range", "FLOAT"),
            bigquery.SchemaField("condition", "STRING"),
            bigquery.SchemaField("city", "STRING"),
        ],
        write_disposition="WRITE_TRUNCATE",
    )

    df["date"] = df["date"].dt.strftime("%Y-%m-%d")
    job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    logger.info(f"Loaded {len(df)} rows to BigQuery table {table_ref}")

@flow(name="weather-etl")
def run_pipeline():
    logger = get_run_logger()
    logger.info("Pipeline starting")
    data = extract()
    daily = store_raw(data)
    df = transform(daily)
    df = validate(df)
    load_to_bigquery(df)
    logger.info("Pipeline complete")

if __name__ == "__main__":
    run_pipeline()