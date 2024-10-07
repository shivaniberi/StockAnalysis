from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.dates import days_ago
import requests
import json

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
    # Retrieve API key from Airflow Variables
    api_key = Variable.get('VANTAGE_API_KEY')
    symbol = 'MSFT'
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}'
    
    # Make the API request
    response = requests.get(url)
    data = response.json()
    
    return data

# Task 2: Process the data using the @task decorator
@task
def transform(stock_data: dict):
    # Example processing: Extract close prices
    time_series = stock_data.get('Time Series (Daily)', {})
    processed_data = {date: details['4. close'] for date, details in time_series.items()}
    
    # Log the processed data
    print(f"Processed Data: {json.dumps(processed_data, indent=2)}")
    return processed_data

# Define the task dependencies using the decorator functions
with dag:
    stock_data = extract()
    transformed_data = transform(stock_data)
