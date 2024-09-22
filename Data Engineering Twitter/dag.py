from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from X_etl import run_x_etl

## X won't allow for tweet data without v2 API endpoints.

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email': 'your email',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


dag = DAG(
    'X_dag',
    default_args=default_args,
    description='X Airflow DAG'
)

initiate_x_etl = PythonOperator(
    task_id = 'Complete_X_ETL',
    python_callable = run_x_etl,
    dag=dag
)

initiate_x_etl

