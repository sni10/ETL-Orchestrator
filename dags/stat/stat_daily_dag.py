# dags/stat_daily_dag.py
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
    'project_name': 'EDI daily stat',
    'dag_id': 'stat_daily_dag',
    'dag_description': 'EDI generate daily stat',
    'schedule': '0 9 * * *',
    'source': 'edi',

    'database': {
        'source': {
            'table': f'{os.getenv("DB_EDI_SCHEMA_NAME")}.products_output',
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
    stats_service = ServiceFactory.create_stat_service(settings)
    stats_service.run_statistics()


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
