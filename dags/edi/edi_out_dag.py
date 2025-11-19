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
    'dag_id': 'update_insert_upc_prices_kafka_db',
    'dag_description': 'EDI save suppliers UPCs from Kafka to DB',
    'schedule': '*/15 * * * *',

    'broker': {
        'type': 'kafka',
        'kafka_bootstrap_servers': f'{get_str("KAFKA_EDI_HOST")}:{get_int("KAFKA_EDI_PORT")}',
        'topic': 'edi_output',
        'consumer_group': 'edi_generate_upc_consumer_group'
    },

    'message_filter': [
        {
            'column': 'status',
            'operation': 'not in',
            'values': [
                'Receiving',
                'Dropship Fee',
                'Shipping',
            ]
        }
    ],

    'database': {
        'batch_size': 100,
        'queue_size_limit': 1000,
        'version': {
            'column': 'version',
        },
        'target': {
            'table': f'{get_str("DB_EDI_SCHEMA_NAME")}.products_output',
            'conflict_key': 'upc, supplier_id',
            'columns': [
                'version',
                'upc',
                'price',
                'qty',
                'supplier_id'
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

service = ServiceFactory.create_loading_service(settings)


def update_data(**context):
    service.load_data()
    context['task_instance'].xcom_push(key='rows', value=service.rows)


def delete_data(**context):
    ti = context['task_instance']
    rows = ti.xcom_pull(task_ids='update_data', key='rows')
    service.delete_data(rows)


dag = DAG(
    dag_id=settings['dag_id'],
    description=settings['dag_description'],
    schedule_interval=settings['schedule'],
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['edi']
)

task_update_data = PythonOperator(
    task_id='update_data',
    python_callable=update_data,
    provide_context=True,
    dag=dag,
)

task_delete_data = PythonOperator(
    task_id='delete_data',
    python_callable=delete_data,
    provide_context=True,
    dag=dag,
)

task_update_data >> task_delete_data
