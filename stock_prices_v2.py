
from airflow import DAG
from airflow.operators.python import PythonOperator
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
dag = DAG(
    'homework_4_v2',
    default_args=default_args,
    description='A simple DAG to fetch stock data and process it',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
)

# Task 1: Fetch stock data from Alpha Vantage
def fetch_stock_data(**kwargs):
    # Retrieve environment variables (set these in Airflow connections or environment variables)
    api_key = os.getenv('VANTAGE_API_KEY')
    symbol = 'MSFT'
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}'
    
    # Make the API request
    response = requests.get(url)
    data = response.json()
    
    # Save the response for downstream processing (XCom)
    kwargs['ti'].xcom_push(key='stock_data', value=data)

# Task 2: Process the data
def process_data(**kwargs):
    # Retrieve the stock data from XCom
    stock_data = kwargs['ti'].xcom_pull(key='stock_data', task_ids='fetch_stock_data')
    
    # Example processing: Extract close prices
    time_series = stock_data.get('Time Series (Daily)', {})
    processed_data = {date: details['4. close'] for date, details in time_series.items()}
    
    # Log the processed data or store it in Snowflake (as per your logic)
    print(f"Processed Data: {json.dumps(processed_data, indent=2)}")

# Define tasks using PythonOperator
fetch_task = PythonOperator(
    task_id='fetch_stock_data',
    python_callable=fetch_stock_data,
    provide_context=True,
    dag=dag,
)

process_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    provide_context=True,
    dag=dag,
)

# Set the task dependencies
fetch_task >> process_task
