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
    'dag_id': 'select_empty_asisn_db_kafka',
    'dag_description': 'Prepare to kafka upc with empty asin to parser amazon API',
    'schedule': '30 */1 * * *',

    'broker': {
        'type': 'kafka',
        'kafka_bootstrap_servers': f'{get_str("KAFKA_EDI_HOST")}:{get_int("KAFKA_EDI_PORT")}',
        'topic': 'upc_in_input',
        'consumer_group': 'in_upc_consumer_group'
    },

    'message_filter': [],

    'database': {
        'batch_size': 10000,
        'queue_size_limit': 10000,
        'version': {
            'column': 'version_asin',
        },
        'back_update': {
            'column': 'upc',
        },
        'order': {
            'column': 'version_asin',
        },
        'source': {
            'table': f'{get_str("DB_EDI_SCHEMA_NAME")}.products_output',
            'columns': {
                'version': 'version',
                'version_asin': 'version_asin',
                'upc': 'upc',
                'supplier_id': 'supplier_id',
                'is_deleted': 'is_deleted',
                'created_at': 'created_at'
            },
            'conditions': [
                {
                    'column': 'asin',
                    'operation': 'IS',
                    'value': 'NULL'
                },
                {
                    'column': 'asin_upcs',
                    'operation': 'IS',
                    'value': 'NULL'
                },
                {
                    'column': 'version_asin',
                    'operation': '<',
                    'value': 24
                },
                {
                    'column': 'is_deleted',
                    'operation': 'IS',
                    'value': 'NULL'
                },
            ],
        },
        'target': {
            'table': f'{get_str("DB_EDI_SCHEMA_NAME")}.products_output',
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
    tags=['upc']
)

init_dag = PythonOperator(
    task_id='run_dag',
    python_callable=ingest_data,
    dag=dag,
)

init_dag
