from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.parser import parse_and_insert

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='korzina_parser_dag',
    default_args=default_args,
    start_date=datetime.now() - timedelta(days=1), 
    schedule_interval='0 8 * * *',
    catchup=False,
    description='Daily parser for korzinavdom.kz with ClickHouse insert'
) as dag:

    task_parse = PythonOperator(
        task_id='parse_korzina',
        python_callable=parse_and_insert,
    )
