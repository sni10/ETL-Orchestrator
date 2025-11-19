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
    'project_name': 'New ASINS Output',
    'dag_id': 'new_asin_keyword_out',
    'dag_description': 'EDI save suppliers UPCs from Kafka to DB',
    'schedule': '*/11 * * * *',
    'source': 'edi',

    'record_lifetime': int(os.getenv("RECORD_LIFETIME", 3)),

    "broker": {
        'type': 'rabbit',
        "host": get_str("RABBITMQ_EDI_HOST"),
        "port": get_int("RABBITMQ_EDI_PORT"),
        "username": get_str("RABBITMQ_EDI_USER"),
        "password": get_str("RABBITMQ_EDI_PASS"),
        'topic': 'new_asins_keyword',
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
        'batch_size': 10000,
        'queue_size_limit': 500000,
        'source': {
            'table': f'{os.getenv("DB_EDI_SCHEMA_NAME")}.amazon_products',
            'conflict_key': ['external_id', 'marketplace_id'],
            'columns': {
                'external_id': 'external_id',
                'name': 'name',
                'brand': 'brand',
                's_keyword': 's_keyword',
                's_brand': 's_brand',
                'attributes': 'attributes',
                'parent_external_id': 'parent_external_id',
                'has_variations': 'has_variations',
                'version': 'version',
                'lang': 'lang',
                'marketplace_id': 'marketplace_id'
            },
            'conditions': [],
        },
        'target': {
            'table': f'{os.getenv("DB_EDI_SCHEMA_NAME")}.amazon_products',
        },
        'connection': {
            'host': get_str('DB_EDI_HOST'),
            'port': get_int('DB_EDI_PORT'),
            'user': get_str('DB_EDI_USER'),
            'password': get_str('DB_EDI_PASSWORD'),
            'dbname': get_str('DB_EDI_NAME')
        },
    }
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
    tags=['new_asin_keyword']
)

task_update_data = PythonOperator(
    task_id='update_data',
    python_callable=update_data,
    provide_context=True,
    dag=dag,
)

task_update_data
