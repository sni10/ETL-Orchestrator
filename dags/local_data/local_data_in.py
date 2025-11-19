# dags/ingestion_dag.py
import os
from datetime import datetime
from src.factories.service_factory import ServiceFactory
from airflow import DAG
from airflow.operators.python import PythonOperator
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


def map_before_kafka_produce(data: list[tuple], version: int) -> dict:
    asins = []
    marketplaces = {}
    for row in data:
        catalog_id, external_id, marketplaceid, uniq_asin_version = row
        asins.append(external_id)
        marketplaces[catalog_id] = marketplaceid

    return {
        "asins": asins,
        "marketplaces": marketplaces,
        "version": version
    }


settings = {
    'project_name': 'Local Data',
    'dag_id': 'listing_tasks_for_parser',
    'dag_description': 'Local Data generate listing messages for parsing',
    'schedule': '11 */8 * * *',

    'broker': {
        'type': 'rabbit',
        'host': get_str("RABBITMQ_EDI_HOST"),
        'port': get_int("RABBITMQ_EDI_PORT"),
        'username': get_str("RABBITMQ_EDI_USER"),
        'password': get_str("RABBITMQ_EDI_PASS"),
        'topic': 'listing_in_local_data'
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
        'batch_size': 1000,
        'queue_size_limit': 10000,
        'source': {
            'table': f'{get_str("DB_EDI_SCHEMA_NAME")}.listing',
            'conflict_key': ['external_id', 'catalog_id'],
            'columns': {
                'catalog_id': 'catalog_id',
                'external_id': 'external_id',
                'marketplaceid': 'marketplaceid',
                'version': 'version'
            },
            'conditions': [
                {
                    'column': 'attributes',
                    'operation': 'IS',
                    'value': 'NULL'
                },
                {
                    'op': 'AND',
                    'column': 'version',
                    'operation': '<',
                    'value': 1
                }
            ],
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
    'use_offset_tracking': False,
    'map_before_kafka_produce': map_before_kafka_produce
}


def ingest_data():
    validate_settings(settings)
    service = ServiceFactory.create_ingestion_service(settings)
    service.ingest_data()


dag = DAG(
    dag_id=settings['dag_id'],
    description=settings['dag_description'],
    schedule_interval=settings['schedule'],
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['local_data']
)

ingest_data_operator = PythonOperator(
    task_id='ingest_data',
    python_callable=ingest_data,
    dag=dag,
)


ingest_data_operator
