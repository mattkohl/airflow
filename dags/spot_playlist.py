from datetime import datetime, timedelta
import json
from typing import Dict

from airflow.hooks.http_hook import HttpHook
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG


def playlist_run(playlist_uri: str):
    api_hook = HttpHook(http_conn_id='seed_api', method='GET')
    resp = api_hook.run(f'playlists/{playlist_uri}/run')
    return [t["spot_uri"] for t in json.loads(resp.content)]


def track_run(track_uri: str) -> Dict:
    api_hook = HttpHook(http_conn_id='seed_api', method='GET')
    resp = api_hook.run(f'tracks/{track_uri}/run')
    return json.loads(resp.content)


def run_tracks(**context):
    for i, track_uri in enumerate(context['task_instance'].xcom_pull(task_ids='run_playlist')):
        print(f"Now running track {i}: {track_uri}")
        track_run(track_uri)


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

dag = DAG(dag_id='spot_playlist_tracks', default_args=default_args)

run_playlist_task = \
    PythonOperator(task_id='run_playlist',
                   python_callable=playlist_run,
                   op_kwargs={"playlist_uri": "spotify:user:matt.kohl-gb:playlist:5eyl25gqWwIERlx93pVDnU"},
                   dag=dag)

run_playlist_tracks_task = \
    PythonOperator(task_id=f'run_playlist_tracks',
                   python_callable=run_tracks,
                   provide_context=True,
                   dag=dag)

run_playlist_task >> run_playlist_tracks_task
