from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from twitter_etl import run_twitter_etl


#default arguments of dag creation
default_args = {
    'owner': 'airflow-kaushik',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 8),
    'email': ['airflow@example.com', 'kaushikdey1984@yahoo.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

#dag settings 
dag = DAG(
    'twitter_dag',
    default_args=default_args,
    description='Twitter ETL process DAG!!!',
    schedule_interval=timedelta(days=1),
)

#run the ETL callable function
run_etl = PythonOperator(
    task_id='complete_twitter_etl',
    python_callable=run_twitter_etl,
    dag=dag, 
)

run_etl