from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 5, 30),
    'email': ['hlaford@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('seed_index', default_args=default_args)

t1 = SimpleHttpOperator(task_id="get_seed_index",
                        http_conn_id="seed_api",
                        method='GET',
                        endpoint='',
                        dag=dag)
