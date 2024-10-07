
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
import requests
import json
import os

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Initialize the DAG
with DAG(
    'homework_4_v2_decorator',
    default_args=default_args,
    description='A simple DAG to fetch and process stock data using the @task decorator',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Task 1: Fetch stock data from Alpha Vantage
    @task()
    def fetch_stock_data():
        # Retrieve environment variables (set these in Airflow connections or environment variables)
        api_key = os.getenv('VANTAGE_API_KEY')
        symbol = 'MSFT'
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}'
        
        # Make the API request
        response = requests.get(url)
        data = response.json()
        return data

    # Task 2: Process the data
    @task()
    def process_data(stock_data):
        # Example processing: Extract close prices
        time_series = stock_data.get('Time Series (Daily)', {})
        processed_data = {date: details['4. close'] for date, details in time_series.items()}
        
        # Log the processed data or store it in Snowflake (as per your logic)
        print(f"Processed Data: {json.dumps(processed_data, indent=2)}")
    
    # Define task dependencies using function calls
    stock_data = fetch_stock_data()
    process_data(stock_data)
