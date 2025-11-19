# dags/health_check_trigger_stat.py
import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.factories.service_factory import ServiceFactory

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 22),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3
}

settings = {
    'project_name': 'EDI health check',
    'dag_id': 'edi_health_check',
    'dag_description': 'EDI health check',
    'schedule': '0 */1 * * *',
    'source': 'edi',

    'database': {
        'source': {
            'table': f'{os.getenv("DB_EDI_SCHEMA_NAME")}.products_output',
            'join': f'{os.getenv("DB_EDI_SCHEMA_NAME")}.suppliers',
        },
        'health': {
            'join_field': 'supplier_id',
            'id_field': 'supplier_id',
            'date_field': 'updated_at',
            'hours': 4,
        },
        'connection': {
            'host': os.getenv('DB_EDI_HOST'),
            'port': os.getenv('DB_EDI_PORT'),
            'user': os.getenv('DB_EDI_USER'),
            'password': os.getenv('DB_EDI_PASSWORD'),
            'dbname': os.getenv('DB_EDI_NAME')
        },
    }
}


def run_stat():
    stats_service = ServiceFactory.create_trigger_service(settings)
    stats_service.run_trigger()


dag = DAG(
    dag_id=settings['dag_id'],
    description=settings['dag_description'],
    schedule_interval=settings['schedule'],
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['stat']
)

init_dag = PythonOperator(
    task_id='run_dag',
    python_callable=run_stat,
    dag=dag,
)

init_dag
