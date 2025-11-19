# dags/ingestion_dag.py
import os
from datetime import datetime
from src.factories.service_factory import ServiceFactory
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.utils.env import get_int, get_str

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 22),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3
}

settings = {
    'project_name': 'Keywords brands input',
    'dag_id': 'new_brands_keywords',
    'dag_description': 'Local Data generate listing messages for parsing',
    'schedule': '*/9 * * * *',
    'source': 'edi',
    'new_version': False,

    "broker": {
        'type': 'rabbit',
        "host": get_str("RABBITMQ_EDI_HOST"),
        "port": get_int("RABBITMQ_EDI_PORT"),
        "username": get_str("RABBITMQ_EDI_USER"),
        "password": get_str("RABBITMQ_EDI_PASS"),
        'topic': 'new_brands_keywords',
    },

    'database': {
        'default_values': {
        },
        'order': {
            'column': 'version',
        },
        'version': {
            'column': 'version',
            'scale': 1000000,
        },
        'batch_size': 1000,
        'queue_size_limit': 10000,
        'source': {
            'table': f'{os.getenv("DB_EDI_SCHEMA_NAME")}.asin_input_queue',
            'conflict_key': ['keyword', 'brand'],
            'columns': {
                'keyword': 'keyword',
                'brand': 'brand',
                'version': 'version',
            },
            'conditions': [
                {
                    'column': 'version',
                    'operation': '<',
                    'value': 48
                }
            ],
        },
        'target': {
            'table': f'{os.getenv("DB_EDI_SCHEMA_NAME")}.asin_input_queue',
        },
        'connection': {
            'host': get_str('DB_EDI_HOST'),
            'port': get_int('DB_EDI_PORT'),
            'user': get_str('DB_EDI_USER'),
            'password': get_str('DB_EDI_PASSWORD'),
            'dbname': get_str('DB_EDI_NAME')
        }
    }
}


def ingest_data():
    service = ServiceFactory.create_ingestion_service(settings)
    service.ingest_data()

dag = DAG(
    dag_id=settings['dag_id'],
    description=settings['dag_description'],
    schedule_interval=settings['schedule'],
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['new_asin_keyword']
)

ingest_data_operator = PythonOperator(
    task_id='ingest_data',
    python_callable=ingest_data,
    dag=dag,
)


ingest_data_operator

