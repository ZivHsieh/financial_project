import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pytz

# Load environment variables
load_dotenv()

# MySQL connection details from .env
mysql_ip = os.getenv("DB_HOST")
mysql_port = os.getenv("DB_PORT")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
database = os.getenv("DB_NAME")
source_table = "dividend_rawdata"
target_table = "dividend_yield"

# Create the connection string and engine
connection_string = f"mysql+pymysql://{user}:{password}@{mysql_ip}:{mysql_port}/{database}"

def create_mysql_engine():
    """Creates and returns a MySQL engine connection."""
    return create_engine(connection_string)

def convert_to_gregorian(minguo_date):
    """Converts Minguo date format (e.g., '102年10月24日') to Gregorian format."""
    try:
        minguo_date = minguo_date.strip()
        year_str, rest = minguo_date.split("年", 1)
        year = int(year_str) + 1911
        return f"{year}年{rest}"
    except Exception as e:
        print(f"日期轉換錯誤: {minguo_date}, 錯誤: {e}")
        return None

def process_and_store_data():
    """Fetches, processes, and stores data into the target MySQL table."""
    engine = create_mysql_engine()

    # Truncate the target table before inserting new data
    with engine.connect() as connection:
        connection.execute(f"TRUNCATE TABLE {target_table}")

    # Query data from the source table
    query = f"""
        SELECT ex_dividend_date, 
               stock_code, 
               pre_ex_dividend_close_price,
               rights_dividend_value
        FROM {source_table}"""

    try:
        # Load data into a DataFrame
        data = pd.read_sql(query, con=engine)

        # Add a new column for dividend_yield
        data['dividend_yield'] = round((data['rights_dividend_value'] / data['pre_ex_dividend_close_price'] * 100), 3)

        # Clean and convert the date column
        data["ex_dividend_date"] = data["ex_dividend_date"].apply(lambda x: x.strip() if isinstance(x, str) else x)
        data["ex_dividend_date"] = data["ex_dividend_date"].apply(convert_to_gregorian)
        data["ex_dividend_date"] = pd.to_datetime(data["ex_dividend_date"], format="%Y年%m月%d日", errors='coerce')

        # Filter out rows with invalid dates
        valid_data = data.dropna(subset=["ex_dividend_date"])

        # Store the processed data back to MySQL
        valid_data.to_sql(name=target_table, con=engine, if_exists='append', index=False)

        print("Data processed and stored successfully.")
    except Exception as e:
        print(f"Error occurred: {e}")

# Airflow DAG settings
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 4, 19, 0, tzinfo=pytz.timezone('Asia/Taipei')),  # Start date
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dividend_yield_calculation',
    default_args=default_args,
    schedule_interval='0 19 * * *',  # Run daily at 19:00 Taipei time
    catchup=False
) as dag:
    task = PythonOperator(
        task_id='process_and_store_dividend_data',
        python_callable=process_and_store_data
    )
