# dags/loading_dag.py

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
    'dag_id': 'save_finded_asisn_kafka_db',
    'dag_description': 'Update UPS new ASIN',
    'schedule': '*/45 * * * *',

    'broker': {
        'type': 'kafka',
        'kafka_bootstrap_servers': f'{get_str("KAFKA_EDI_HOST")}:{get_int("KAFKA_EDI_PORT")}',
        'topic': 'upc_in_output',
        'consumer_group': 'out_upc_asin_consumer_group'
    },

    'message_filter': [],

    'database': {
        'batch_size': 1000,
        'queue_size_limit': 10000,
        'version': {
            'column': 'version_asin',
        },
        'target': {
            'table': f'{get_str("DB_EDI_SCHEMA_NAME")}.products_output',
            'conflict_key': 'upc,supplier_id',
            'columns': [
                'upc',
                'asin',
                'asin_upcs',
                'supplier_id',
                'is_deleted',
                'created_at',
                'version_asin',
                'brand',
                'name',
                'lang',
                'marketplace_id',
                'version'
            ]
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
    service = ServiceFactory.create_loading_service(settings)
    service.up_data()


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
    python_callable=update_data,
    dag=dag,
)

init_dag
