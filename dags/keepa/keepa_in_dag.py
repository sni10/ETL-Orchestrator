# dags/ingestion_dag.py
import os
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.factories.service_factory import ServiceFactory
from datetime import datetime, timedelta, timezone
from src.utils.env import get_int, get_str


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 22),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3
}


def saparate_task_to_step(data=None):
    if data is None:
        return {}
    task = data.copy()
    tz = timezone.utc
    now = datetime.now(tz)

    try:
        if not data.get("version") or data["version"] == '':
            time_from = datetime.strptime(data["time_from"], "%Y-%m-%d").replace(tzinfo=tz)
        else:
            time_from = datetime.fromtimestamp(data["version"] / 1000, tz=timezone.utc)
            time_from_stored = datetime.strptime(time_from.strftime("%Y-%m-%d"), "%Y-%m-%d").replace(tzinfo=tz)
            now_from_stored = datetime.strptime(now.strftime("%Y-%m-%d"), "%Y-%m-%d").replace(tzinfo=tz)
            if time_from_stored >= now_from_stored:
                time_from = datetime.strptime(data["time_from"], "%Y-%m-%d").replace(tzinfo=tz)

    except Exception as e:
        print('!!! Exception:' + str(e))
        time_from = now

    time_to = time_from + timedelta(hours=data["step"])

    version = int(time_to.timestamp() * 1000)

    status = "IN_PROGRESS"
    if data.get("time_to"):
        time_to_stored = datetime.strptime(data["time_to"], "%Y-%m-%d").replace(tzinfo=tz)
        if time_to <= time_to_stored:
            status = "FINISHED"

    task["time_from"] = time_from.strftime("%Y-%m-%d")
    task["time_to"] = time_to.strftime("%Y-%m-%d")
    task["version"] = data["version"] = version
    task["status"] =  data["status"] = status

    return data, task


settings = {
    'project_name': 'KEEPA',
    'dag_id': 'keepa_generate',
    'dag_description': 'KEEPA generate',
    'schedule': '*/30 * * * *',

    'broker': {
        'type': 'rabbit',
        'host': get_str("RABBITMQ_EDI_HOST"),
        'port': get_int("RABBITMQ_EDI_PORT"),
        'username': get_str("RABBITMQ_EDI_USER"),
        'password': get_str("RABBITMQ_EDI_PASS"),
        'topic': 'keepa_input'
    },

    'message_filter': [],

    'database': {
        'default_values': {
            'updated_at': 'CURRENT_TIMESTAMP'
        },
        'queue_size_limit': 3000,
        'batch_size': 1000,
        'order': {
            'column': 'version',
        },
        'version': {
            'column': 'version',
            'scale': 1000,
        },
        'source': {
            'table': f'{get_str("DB_EDI_SCHEMA_NAME")}.search_requests',
            'conflict_key': ['id'],
            'columns': {
                'id': 'id',
                'domain_id': 'domain_id',
                'brand': 'brand',
                'time_from': 'time_from',
                'time_to': 'time_to',
                'version': 'version',
                'status': 'status',
                'step': 'step'
            },
            'conditions': [
                {
                    'column': 'is_active',
                    'operation': '=',
                    'value': 'TRUE'
                },
                {
                    'op': 'AND',
                    'column': 'version',
                    'operation': '<=',
                    'value': 0
                },
                {
                    'op': 'AND',
                    'column': 'status',
                    'operation': 'IS DISTINCT FROM',
                    'value': 'FINISHED'
                }
            ],
        },
        'target': {
            'table': f'{get_str("DB_EDI_SCHEMA_NAME")}.search_requests',
        },
        'connection': {
            'host': get_str('DB_EDI_HOST'),
            'port': get_int('DB_EDI_PORT'),
            'user': get_str('DB_EDI_USER'),
            'password': get_str('DB_EDI_PASSWORD'),
            'dbname': get_str('DB_EDI_NAME')
        }
    },
    'new_version': False,
    'use_offset_tracking': False,
    'modify_data_function': saparate_task_to_step
}

def ingest_data():
    service = ServiceFactory.create_ingestion_service(settings)
    service.ingest_data()


dag = DAG(
    dag_id=settings['dag_id'],
    description=settings['dag_description'],
    schedule=settings['schedule'],
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['keepa']
)

init_dag = PythonOperator(
    task_id='run_dag',
    python_callable=ingest_data,
    dag=dag,
)

init_dag
