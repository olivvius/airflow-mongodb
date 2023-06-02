from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
import pandas as pd
import pymongo
from airflow.models import TaskInstance

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 4, 10),
    'retries': 0,
    'retry_delay': timedelta(seconds=10)
}

dag = DAG(
    'data_pipeline',
    default_args=default_args,
    description='Pipeline pour générer, traiter et stocker des données',
    schedule_interval=timedelta(seconds=10)
)

def generate_data():
    # Création d'un DataFrame vide avec les colonnes attendues
    df = pd.DataFrame(columns=["gender", "first_name", "last_name"])

    # Boucle pour effectuer 3 requêtes à l'API
    for i in range(3):
        # Requête à l'API pour récupérer des données aléatoires
        response = requests.get("https://randomuser.me/api/").json()

        # Extraction des données intéressantes
        gender = response["results"][0]["gender"]
        first_name = response["results"][0]["name"]["first"]
        last_name = response["results"][0]["name"]["last"]

        # Ajout des données au DataFrame
        new_row = {"gender": gender, "first_name": first_name, "last_name": last_name}
        df = df.append(new_row, ignore_index=True)

    # Stockage du DataFrame en mémoire et envoi aux tâches suivantes
    return df.to_json()

def process_data(**context):
    # Récupération du DataFrame stocké en mémoire
    df_json = context['ti'].xcom_pull(task_ids="generate_data_task", key="my_data_frame")
    df = pd.read_json(df_json)

    # Traitement des données
    df = df[df['gender'] == 'female'][['first_name', 'last_name']]

    # Envoi du DataFrame traité aux tâches suivantes
    context['ti'].xcom_push(key="my_processed_data_frame", value=df.to_json())

def store_data(**context):
    # Récupération du DataFrame traité stocké en mémoire
    df_json = context['ti'].xcom_pull(task_ids="process_data_task", key="my_processed_data_frame")
    df = pd.read_json(df_json)

    # Conversion du DataFrame en liste de dictionnaires
    data = df.to_dict('records')

    # Connexion à la base de données MongoDB
    client = pymongo.MongoClient("mongodb://mongo:27017/")
    db = client["mydatabase"]
    collection = db["mycollection"]

    # Stockage des données dans la base MongoDB
    collection.insert_many(data)

generate_data_task = PythonOperator(
    task_id='generate_data_task',
    python_callable=generate_data,
    dag=dag
)

process_data_task = PythonOperator(
    task_id='process_data_task',
    python_callable=process_data,
    provide_context=True,
    dag=dag
)

store_data_task = PythonOperator(
    task_id='store_data_task',
    python_callable=store_data,
    provide_context=True,
    dag=dag
)

generate_data_task >> process_data_task >> store_data_task
