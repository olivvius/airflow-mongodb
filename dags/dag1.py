from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 4, 10),
    'retries': 0,
    'retry_delay': timedelta(seconds=10)
}

dag = DAG(
    'pyspark_data_pipeline',
    default_args=default_args,
    description='Pipeline pour générer, traiter et stocker des données avec PySpark',
    schedule_interval=timedelta(seconds=10)
)

def generate_data():
    # Chemin d'accès complet du script 'generate_data.py'
    exec(open('/opt/airflow/dags/scripts/generate_data.py').read())

def process_data():
    # Chemin d'accès complet du script 'process_data.py'
    exec(open('/opt/airflow/dags/scripts/process_data.py').read())

def store_data():
    # Chemin d'accès complet du script 'store_data.py'
    exec(open('/opt/airflow/dags/scripts/store_data.py').read())

generate_data_task = PythonOperator(
    task_id='generate_data',
    python_callable=generate_data,
    dag=dag
)

process_data_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    dag=dag
)

store_data_task = PythonOperator(
    task_id='store_data',
    python_callable=store_data,
    dag=dag
)

generate_data_task >> process_data_task >> store_data_task
