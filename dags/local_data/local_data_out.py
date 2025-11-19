# dags/loading_dag.py
import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.factories.service_factory import ServiceFactory
from src.utils.env import get_int, get_str
from src.validators.config_validator import validate_settings

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 22),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3
}

settings = {
    'project_name': 'Local Data',
    'dag_id': 'parsed_data_to_listing',
    'dag_description': 'EDI save suppliers UPCs from Kafka to DB',
    'schedule': '2 */1 * * *',

    'broker': {
        'type': 'rabbit',
        'host': get_str("RABBITMQ_EDI_HOST"),
        'port': get_int("RABBITMQ_EDI_PORT"),
        'username': get_str("RABBITMQ_EDI_USER"),
        'password': get_str("RABBITMQ_EDI_PASS"),
        'topic': 'local_data_output'
    },

    'message_filter': [],

    'database': {
        'default_values': {
            'updated_at': 'CURRENT_TIMESTAMP'
        },
        'order': {
            'column': 'version',
        },
        'version': {
            'column': 'version',
            'scale': 1000000,
        },
        'batch_size': 10000,
        'queue_size_limit': 500000,
        'source': {
            'table': f'{get_str("DB_EDI_SCHEMA_NAME")}.listing',
            'conflict_key': ['external_id', 'catalog_id'],
            'columns': {
                'catalog_id': 'catalog_id',
                'external_id': 'external_id',
                'name': 'name',
                'brand': 'brand',
                'attributes': 'attributes',
                'parent_external_id': 'parent_external_id',
                'has_variations': 'has_variations',
                'marketplaceid': 'marketplaceid',
                'version': 'version'
            },
            'conditions': [],
        },
        'target': {
            'table': f'{get_str("DB_EDI_SCHEMA_NAME")}.listing',
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


def update_data():
    validate_settings(settings)
    service = ServiceFactory.create_loading_service(settings)
    service.load_data()


dag = DAG(
    dag_id=settings['dag_id'],
    description=settings['dag_description'],
    schedule_interval=settings['schedule'],
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['local_data']
)

task_update_data = PythonOperator(
    task_id='update_data',
    python_callable=update_data,
    provide_context=True,
    dag=dag,
)

task_update_data
