from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

from ingest_script import ingest_data


AIRFLOW_HOME = os.getenv('AIRFLOW_HOME', '/opt/airflow/')

URL_PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip-data"
URL_TEMPLATE = URL_PREFIX +  '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
OUTPUT_FILE = AIRFLOW_HOME + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}_raw.parquet'

POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')

POSTGRES_DB_TAXI = os.getenv('POSTGRES_DB_TAXI')

local_workflow = DAG(
    "LocalIngestingDAG",
    schedule_interval="0 0 1 * *",
    start_date = datetime(2020, 12, 2),
    # end_date = datetime(2021, 2, 2),
    catchup=False
)

with local_workflow:
    wget_task = BashOperator(
        task_id = "Wgetdata",
        bash_command = F'curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE}'
        # bash_command= 'echo "{{ execution_date }}"'
    ) 

    ingest_task = PythonOperator(
        task_id = "Ingest",
        python_callable= ingest_data,
        op_kwargs=dict(
            user=POSTGRES_USER,
            password = POSTGRES_PASSWORD,
            host = POSTGRES_HOST,
            port = POSTGRES_PORT, 
            db = POSTGRES_DB_TAXI, 
            table_name = "",
            parquet_file = OUTPUT_FILE

        )
    )

    wget_task >> ingest_task


