from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
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
    'start_date': datetime(2024, 8, 1, 20, 00),
    'email_on_failure': True,
    'email': 'your-email@example.com',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Movie-pipeline2',
    default_args=default_args,
    schedule_interval='00 19 * * 1-5', 
)

# fetch articles
def movie_analysis():
    data = movie_analyze()
    print("Return from fetch articles:", data)
    # ti.xcom_push(key='return_value', value=data)
    # Code to scrape articles from yourstory.com and finshots.in for keywords HDFC and Tata Motors

t2 = PythonOperator(
    task_id='movie_analysis',
    python_callable=movie_analysis,
    dag=dag,
)

# Wait for the 'end' task in the 'first_dag' to complete
wait_for_first_dag = ExternalTaskSensor(
    task_id='wait_for_first_dag',
    external_dag_id='Analyze-pipelines',
    external_task_id='scrape_analyze',
    mode='poke',  # or 'reschedule'
    timeout=600,
    poke_interval=60,
    dag=dag,
)

wait_for_first_dag >> t2