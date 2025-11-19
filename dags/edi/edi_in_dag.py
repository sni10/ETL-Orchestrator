# dags/ingestion_dag.py
import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.factories.service_factory import ServiceFactory
from src.utils.env import get_str, get_int

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 22),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3
}

settings = {
    'project_name': 'EDI generate ASIN',
    'dag_id': 'get_supplier_url_for_generate_upc_db_kafka',
    'dag_description': 'EDI generate suppliers urls for edi scraper from DB to Kafka',
    'schedule': '11 */1 * * *',

    'broker': {
        'type': 'kafka',
        'kafka_bootstrap_servers': f'{get_str("KAFKA_EDI_HOST")}:{get_int("KAFKA_EDI_PORT")}',
        'topic': 'edi_input',
        'consumer_group': 'edi_generate_upc_consumer_group'
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
        'back_update': {
            'column': 'supplier_id',
        },
        'batch_size': 1000,
        'queue_size_limit': 10000,
        'source': {
            'table': f'{get_str("DB_EDI_SCHEMA_NAME")}.suppliers',
            'columns': {
                'supplier_id': 'supplier_id',
                'name': 'name',
                'active': 'active',
                'type_id': 'type_id',
                'source': 'source',
                'column_map_rules': 'column_map_rules',
                'range': 'range',
                'version': 'version',
            },
            'conditions': [
                {
                    'column': 'active',
                    'operation': '=',
                    'value': 'TRUE'
                },
                {
                    'column': 'version',
                    'operation': '<',
                    'value': 1
                }
            ],
        },
        'target': {
            'table': f'{get_str("DB_EDI_SCHEMA_NAME")}.suppliers',
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
    service = ServiceFactory.create_ingestion_service(settings)
    service.ingest_data()


dag = DAG(
    dag_id=settings['dag_id'],
    description=settings['dag_description'],
    schedule_interval=settings['schedule'],
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['edi']
)

init_dag = PythonOperator(
    task_id='run_dag',
    python_callable=ingest_data,
    dag=dag,
)

init_dag
