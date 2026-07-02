# Weather Pipeline

A Python ETL pipeline that extracts live weather forecast data from the Open-Meteo API, transforms it using pandas, and loads it into a PostgreSQL database.

## What it does
- Extracts 7-day weather forecast for Johannesburg from the Open-Meteo API
- Transforms the data — calculates daily temperature range, classifies conditions, converts types
- Loads the cleaned data into a local PostgreSQL database

## Tech stack
- Python 3.12
- pandas
- requests
- psycopg2
- PostgreSQL

## How to run it

1. Clone the repo
2. Create a virtual environment and activate it
3. Install dependencies
4. Set up PostgreSQL and create the database
5. Run the pipeline

\```bash
git clone https://github.com/yourusername/weather-pipeline.git
cd weather-pipeline
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python pipeline.py
\```

