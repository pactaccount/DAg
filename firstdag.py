from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import timedelta, datetime
import requests

# Function to return Snowflake connection
def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

# Task to fetch stock prices
@task
def stockprices(symbol, url):
    r = requests.get(url)
    data = r.json()
    results = []
    for d in data['Time Series (Daily)']:
        stock_info = data['Time Series (Daily)'][d]
        stock_info['date'] = d
        results.append(stock_info)
    return results

# Task to transform data
@task
def transform(prices):
    transformed_data = []
    for price in prices:
        price['source'] = 'API'
        transformed_data.append(price)
    return transformed_data

# Task to load data into Snowflake
@task
def load(prices, symbol):
    cur = return_snowflake_conn()
    try:
        cur.execute('BEGIN')
        cur.execute(f"""CREATE TABLE IF NOT EXISTS Time_series.raw_data.stockprice(
            date DATE,
            open NUMBER(8,5),
            high NUMBER(8,5),
            low NUMBER(8,5),
            close NUMBER(8,5),
            volume INTEGER,
            symbol VARCHAR)""")
    except Exception as e:
        cur.execute('ROLLBACK')
        print(f"Error during table creation: {e}")
        return

    try:
        for p in prices:
            Open = p["1. open"]
            High = p["2. high"]
            Low = p["3. low"]
            Close = p["4. close"]
            Volume = p["5. volume"]
            Date = p["date"]
            insert_sql = f"""
                INSERT INTO raw_data.stockprice (date, open, high, low, close, volume, symbol)
                VALUES ('{Date}', {Open}, {High}, {Low}, {Close}, {Volume}, '{symbol}')
            """
            cur.execute(insert_sql)
            print(f"Inserted: {insert_sql}")
        cur.execute('COMMIT')
    except Exception as e:
        cur.execute('ROLLBACK')
        print(f"Error during data load: {e}")

# DAG definition
default_args = {
    'owner': 'Abhinav',
    'email': ['123@amail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='stock_prices',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ETL'],
    schedule_interval='*/10 * * * *',
    default_args=default_args,
) as dag:

    url = Variable.get("apikey")  # Fetch API key from Airflow Variables
    symbol = 'IBM'  # Stock symbol, can be dynamic if needed

    # Task dependency chain
    prices = stockprices(symbol, url)
    transformed_prices = transform(prices)
    load(transformed_prices, symbol)

