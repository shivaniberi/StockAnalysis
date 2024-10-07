from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.dates import days_ago
import requests
import json
from datetime import datetime, timedelta

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Initialize the DAG
dag = DAG(
    'stockprices_v2_decorator',
    default_args=default_args,
    description='A simple DAG to fetch stock data and process it using @task decorator with Snowflake',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
)

# Function to return a Snowflake connection
def return_snowflake_conn():
    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()

# Task 1: Fetch stock data from Alpha Vantage using the @task decorator
@task
def extract():
    # Retrieve API key and URL template from Airflow Variables
    api_key = Variable.get('VANTAGE_API_KEY')
    url_template = Variable.get("url")
    
    # Define the symbol
    symbol = 'MSFT'
    
    # Format the URL with the desired symbol and API key
    url = url_template.format(symbol=symbol, vantage_api_key=api_key)
    
    # Make the API request
    response = requests.get(url)
    data = response.json()
    
    return data

# Task 2: Get the last 90 days of stock prices
@task
def return_last_90d_price(symbol):
    # Retrieve API key from Airflow Variables
    vantage_api_key = Variable.get('VANTAGE_API_KEY')
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={vantage_api_key}'

    r = requests.get(url)
    data = r.json()

    results = []  # List to hold the last 90 days of stock info
    ninety_days_ago = datetime.today() - timedelta(days=90)  # Get the date 90 days ago

    # Iterate through the daily data and filter for the last 90 days along with date info
    for d in data.get("Time Series (Daily)", {}):
        date_obj = datetime.strptime(d, "%Y-%m-%d")
        if date_obj >= ninety_days_ago:
            # Append the data in clear format using a result dictionary
            price_data = {
                "date": d,
                "open": data["Time Series (Daily)"][d]["1. open"],
                "high": data["Time Series (Daily)"][d]["2. high"],
                "low": data["Time Series (Daily)"][d]["3. low"],
                "close": data["Time Series (Daily)"][d]["4. close"],
                "volume": data["Time Series (Daily)"][d]["5. volume"],
                "symbol": symbol
            }
            results.append(price_data)

    return results

# Task 3: Process the data using the @task decorator
@task
def transform(stock_data: dict):
    # Example processing: Extract close prices
    processed_data = []
    for entry in stock_data:
        processed_data.append(entry)
    
    # Log the processed data
    print(f"Processed Data: {json.dumps(processed_data, indent=2)}")
    return processed_data

# Task 4: Load data into Snowflake
@task
def load(cur, records):
    # Check if records is empty
    if not records:
        print("No records to load.")
        return  # Exit if there are no records

    # Define the target table for price data
    target_table = "demoAPI.raw_data.stock_price"

    # Create the table if it does not exist
    cur.execute(f"""
    CREATE OR REPLACE TABLE {target_table} (
      date DATE PRIMARY KEY,
      symbol VARCHAR,
      open NUMBER,
      high NUMBER,
      low NUMBER,
      close NUMBER,
      volume NUMBER
    )
    """)

    # Load records into the table
    for r in records:
        date = r['date']
        symbol = r['symbol']
        open_price = r['open']
        high_price = r['high']
        low_price = r['low']
        close_price = r['close']
        volume = r['volume']

        print(f"Inserting data for {date}: Open={open_price}, Symbol='{symbol}', High={high_price}, Low={low_price}, Close={close_price}, Volume={volume}")

        # Use parameterized INSERT INTO to avoid SQL injection
        sql = f"""
        INSERT INTO {target_table} (date, symbol, open, high, low, close, volume)
        VALUES (TO_DATE('{date}', 'YYYY-MM-DD'), '{symbol}', {open_price}, {high_price}, {low_price}, {close_price}, {volume})
        """
        cur.execute(sql)

# Define the task dependencies using the decorator functions
with dag:
    stock_data = extract()
    last_90_days_data = return_last_90d_price('MSFT')
    transformed_data = transform(last_90_days_data)
    
    # Load the data into Snowflake
    snowflake_cursor = return_snowflake_conn()
    load(snowflake_cursor, transformed_data)
