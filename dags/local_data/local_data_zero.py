# dags/ingestion_dag.py
import os
from datetime import datetime
from src.factories.service_factory import ServiceFactory
from src.utils.env import get_int, get_str
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.validators.config_validator import validate_settings

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 22),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3
}

settings_get = {
    'project_name': 'Local Data',
    'dag_id': 'zero_local_data',
    'dag_description': 'Local Data listing generator from adding',
    'schedule': '*/15 * * * *',

    'broker': {
        'type': 'rabbit',
        'host': get_str("RABBITMQ_EDI_HOST"),
        'port': get_int("RABBITMQ_EDI_PORT"),
        'username': get_str("RABBITMQ_EDI_USER"),
        'password': get_str("RABBITMQ_EDI_PASS"),
        'topic': 'zero_local_data'
    },

    'message_filter': [],

    'database': {
        'default_values': {},
        'batch_size': 10000,
        'queue_size_limit': 500000,
        'order': {
            'column': 'version',
        },
        'version': {
            'column': 'version',
            'scale': 1000000,
        },
        'source': {
            'table': f'{get_str("DB_EDI_SCHEMA_NAME")}.adding',
            'conflict_key': ['asin', 'id_region', 'marketplaceid'],
            'conditions': [
                {
                    'column': 'version',
                    'operation': '<',
                    'value': 24
                }
            ],
            'columns': {
                'asin': 'external_id',
                'id_region': 'id_region',
                'marketplaceid': 'marketplaceid',
                'version': 'version'
            },
            'joins': [
                {
                    'type': 'LEFT',
                    'alias': 'my_shop',
                    'table': f'{get_str("DB_EDI_SCHEMA_NAME")}.shop',
                    'columns': {
                        'catalog_id': 'catalog_id',
                    },
                    'on': [
                        {
                            'left': 'id_region',
                            'right': 'id'
                        }
                    ]
                }
            ],
        },
        'target': {
            'table': f'{get_str("DB_EDI_SCHEMA_NAME")}.adding',
        },
        'connection': {
            'host': get_str('DB_EDI_HOST'),
            'port': get_int('DB_EDI_PORT'),
            'user': get_str('DB_EDI_USER'),
            'password': get_str('DB_EDI_PASSWORD'),
            'dbname': get_str('DB_EDI_NAME')
        }
    },
    'new_version': True,
    'use_offset_tracking': False
}

settings_put = {
    'broker': {
        'type': 'rabbit',
        'host': get_str("RABBITMQ_EDI_HOST"),
        'port': get_int("RABBITMQ_EDI_PORT"),
        'username': get_str("RABBITMQ_EDI_USER"),
        'password': get_str("RABBITMQ_EDI_PASS"),
        'topic': 'zero_local_data'
    },

    'message_filter': [],

    'database': {
        'default_values': {
            'updated_at': 'CURRENT_TIMESTAMP'
        },
        'batch_size': 1000,
        'target': {
            'table': f'{get_str("DB_EDI_SCHEMA_NAME")}.listing',
            'conflict_key': ['catalog_id', 'external_id'],
            'columns': {
                'external_id': 'external_id',
                'catalog_id': 'catalog_id',
                'marketplaceid': 'marketplaceid',
                'version': 'version'
            }
        },
        'source': {
            'table': f'{get_str("DB_EDI_SCHEMA_NAME")}.listing'
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


def ingest_data():
    validate_settings(settings_get)
    service = ServiceFactory.create_ingestion_service(settings_get)
    service.ingest_data()


def update_data():
    validate_settings(settings_put)
    service = ServiceFactory.create_loading_service(settings_put)
    service.load_data()

dag = DAG(
    dag_id=settings_get['dag_id'],
    description=settings_get['dag_description'],
    schedule_interval=settings_get['schedule'],
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['local_data']
)

get_data = PythonOperator(
    task_id='get_data',
    python_callable=ingest_data,
    dag=dag,
)

put_data = PythonOperator(
    task_id='put_data',
    python_callable=update_data,
    dag=dag,
)

get_data >> put_data
