import pandas as pd
from airflow.models import TaskInstance

# Récupération du DataFrame stocké en mémoire
task_instance = TaskInstance(xcom_task_id="generate_data_task", task_id="", execution_date="")
df_json = task_instance.xcom_pull(task_ids="generate_data_task", key="my_data_frame")
df = pd.read_json(df_json)

# Traitement des données
df = df[df['gender'] == 'female'][['first_name', 'last_name']]

# Envoi du DataFrame traité aux tâches suivantes
task_instance.xcom_push(key="my_processed_data_frame", value=df.to_json())
