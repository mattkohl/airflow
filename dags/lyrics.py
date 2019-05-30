from datetime import datetime, timedelta
import time
import json
from typing import Dict

from airflow.hooks.http_hook import HttpHook
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG


def get_tracks():
    api_hook = HttpHook(http_conn_id='seed_api', method='GET')
    resp = api_hook.run(f'tracks/missing-lyrics')
    return [t["spot_uri"] for t in json.loads(resp.content)]


def track_run(track_uri: str) -> Dict:
    api_hook = HttpHook(http_conn_id='seed_api', method='GET')
    resp = api_hook.run(f'tracks/{track_uri}/run')
    return json.loads(resp.content)


def run_tracks(**context):
    for i, track_uri in enumerate(context['task_instance'].xcom_pull(task_ids='get_those_tracks')):
        print(f"Now running track {i}: {track_uri}")
        track_run(track_uri)
        time.sleep(3)


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

dag = DAG(dag_id='tracks_missing_lyrics', default_args=default_args)

get_tracks_missing_lyrics_task = \
    PythonOperator(task_id='get_those_tracks',
                   python_callable=get_tracks,
                   dag=dag)

run_tracks_missing_lyrics_task = \
    PythonOperator(task_id=f'run_those_tracks',
                   python_callable=run_tracks,
                   provide_context=True,
                   dag=dag)

get_tracks_missing_lyrics_task >> run_tracks_missing_lyrics_task
