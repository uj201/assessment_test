from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import sqlite3
from scripts.scrape_and_analyze.pipeline1 import *
from scripts.movie_analysis.pipeline2 import *

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 1, 19, 00),
    'email_on_failure': True,
    'email': 'your-email@example.com',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'Analyze-pipelines',
    default_args=default_args,
    schedule_interval='00 19 * * 1-5',  # Every working day at 7 PM
)

# fetch articles
def scrape_analyze():
    data = scrape_and_analyze()
    print("Return from fetch articles:", data)
    # ti.xcom_push(key='return_value', value=data)
    # Code to scrape articles from yourstory.com and finshots.in for keywords HDFC and Tata Motors

t1 = PythonOperator(
    task_id='scrape_analyze',
    python_callable=scrape_analyze,
    dag=dag,
)

