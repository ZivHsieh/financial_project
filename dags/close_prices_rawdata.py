import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import time
import pandas as pd
import numpy as np
from io import StringIO
import requests
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python import PythonOperator
import pytz
import logging

# Load environment variables from .env
load_dotenv()

# MySQL connection details from .env
mysql_ip = os.getenv("DB_HOST")
mysql_port = os.getenv("DB_PORT")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
database = os.getenv("DB_NAME")
table_name = "stock_prices_rawdata"

# Define column order
COLUMN_ORDER = [
    "record_date", "stock_code", "stock_name", "transaction_volume",
    "transaction_count", "transaction_amount", "opening_price",
    "highest_price", "lowest_price", "closing_price", "price_change_symbol",
    "price_change", "last_bid_price", "last_bid_volume",
    "last_ask_price", "last_ask_volume", "pe_ratio", "additional_info"
]

def create_mysql_engine():
    """Create and return a MySQL engine connection."""
    connection_string = f"mysql+pymysql://{user}:{password}@{mysql_ip}:{mysql_port}/{database}"
    return create_engine(connection_string)

def log_error_to_mysql(error_message: str):
    """Log error message to MySQL error_log table."""
    try:
        engine = create_mysql_engine()
        error_data = pd.DataFrame({
            'error_message': [error_message],
            'timestamp': [datetime.now(pytz.timezone('Asia/Taipei'))],
            'table_name': [table_name]
        })
        error_data.to_sql(name='error_log', con=engine, if_exists='append', index=False)
        logging.error(f"Error logged to error_log table: {error_message}")
    except Exception as e:
        logging.error(f"Failed to log error to MySQL. Error: {str(e)}")

def fetch_data_from_twse(date):
    """Fetch stock price data from TWSE for the specified date, returns CSV string."""
    url = f"http://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date={date.strftime('%Y%m%d')}&type=ALL"
    response = requests.post(url)
    if response.status_code == 200:
        return response.text
    else:
        raise Exception(f"Unable to fetch data from TWSE, HTTP Status: {response.status_code}")

def process_csv_to_dataframe(csv_data, date):
    """Convert CSV string to DataFrame, clean and process columns."""
    df = pd.read_csv(StringIO("\n".join([i.translate({ord(c): None for c in ' '}) 
                                         for i in csv_data.split('\n') 
                                         if len(i.split('",')) == 17 and i[0] != '='])), header=0)
    df.insert(0, 'record_date', date.strftime('%Y-%m-%d'))

    if len(df.columns) == 18:
        df.columns = COLUMN_ORDER
    else:
        logging.error(f"Column count: {len(df.columns)}, Column names: {df.columns.tolist()}")
        raise ValueError("Parsing error: Column count does not match the expected 18 columns")

    columns_to_clean = ['transaction_amount', 'transaction_volume', 'transaction_count',
                        'last_bid_volume', 'last_ask_volume', 'last_bid_price', 
                        'last_ask_price', 'opening_price', 'highest_price', 
                        'lowest_price', 'closing_price', 'price_change']
    for col in columns_to_clean:
        df[col] = df[col].astype(str).str.replace(',', '').replace('--', np.nan).astype(float)

    df['price_change_symbol'] = df['price_change_symbol'].replace({'+': 'plus', '-': 'minus'})
    return df

def fetch_and_store_data(execution_date=None):
    """Main function to fetch and store data from TWSE to MySQL."""
    date = datetime.now(pytz.timezone('Asia/Taipei'))
    fail_count = 0
    allow_continuous_fail_count = 5
    engine = create_mysql_engine()

    while True:
        logging.info(f'Processing {date}')
        try:
            csv_data = fetch_data_from_twse(date)
            df = process_csv_to_dataframe(csv_data, date)
            df = df[COLUMN_ORDER]

            # Check for existing data in the database
            existing_data = pd.read_sql_query(
                f"SELECT record_date, stock_code FROM {table_name} WHERE record_date = '{date.strftime('%Y-%m-%d')}'", 
                con=engine
            )
            if not existing_data.empty:
                df = df.merge(existing_data, on=['record_date', 'stock_code'], how='left', indicator=True)
                df = df[df['_merge'] == 'left_only'].drop(columns=['_merge'])

            if df.empty:
                logging.info(f"No new data to insert for {date}")
                break

            df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
            logging.info('Data inserted successfully!')
            break
        except Exception as e:
            error_message = f"Failed to process {date.strftime('%Y-%m-%d')}: {str(e)}"
            log_error_to_mysql(error_message)
            fail_count += 1
            if fail_count >= allow_continuous_fail_count:
                final_error_message = f"Reached allowed continuous fail count ({allow_continuous_fail_count}). Last attempt date: {date.strftime('%Y-%m-%d')}"
                log_error_to_mysql(final_error_message)
                raise Exception(final_error_message)
            time.sleep(10)

# Airflow DAG settings
default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2024, 10, 29, 18, 0),  # Start date in UTC, equivalent to local time 2024-10-29 18:00
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG('close_prices_rawdata', default_args=default_args, schedule_interval='0 10 * * *', catchup=True) as dag:
    task = PythonOperator(
        task_id='fetch_and_store_data',
        python_callable=fetch_and_store_data
    )
