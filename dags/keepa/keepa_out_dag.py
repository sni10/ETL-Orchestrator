# dags/loading_dag.py
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from src.factories.service_factory import ServiceFactory
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
    'project_name': 'KEEPA',
    'dag_id': 'keepa_save',
    'dag_description': 'EDI KEEPA',
    'schedule': '*/15 * * * *',

    'broker': {
        'type': 'rabbit',
        'host': get_str("RABBITMQ_EDI_HOST"),
        'port': get_int("RABBITMQ_EDI_PORT"),
        'username': get_str("RABBITMQ_EDI_USER"),
        'password': get_str("RABBITMQ_EDI_PASS"),
        'topic': 'keepa_output'
    },

    'message_filter': [],

    'database': {
        'version': {
            'column': 'version',
        },
        'target': {
            'table': f'{get_str("DB_EDI_SCHEMA_NAME")}.find_products',
            'conflict_key': ['search_request_id', 'domain_id', 'brand', 'asin'],
            'columns': {
                'asin': 'asin',
                'brand': 'brand',
                'title': 'title',
                'upc_list': 'upc_list',
                'ean_list': 'ean_list',
                'search_request_id': 'search_request_id',
                'domain_id': 'domain_id',
                'json_data': 'json_data',
                'time_from': 'time_from',
                'time_to': 'time_to',
                'version': 'version',
            }
        },
        'source': {
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
    'use_offset_tracking': False
}




service = ServiceFactory.create_loading_service(settings)

def update_data():
    service.load_data()

dag = DAG(
    dag_id=settings['dag_id'],
    description=settings['dag_description'],
    schedule_interval=settings['schedule'],
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['keepa']
)

task_update_data = PythonOperator(
    task_id='update_data',
    python_callable=update_data,
    provide_context=True,
    dag=dag,
)

task_update_data
