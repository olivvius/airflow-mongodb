import pandas as pd
from pyspark.sql import SparkSession

# Création d'une session Spark
spark = SparkSession.builder.appName("ProcessData").getOrCreate()

# Récupération du DataFrame stocké en mémoire
task_instance = context.get("task_instance")
df_json = task_instance.xcom_pull(task_ids="generate_data_task", key="my_data_frame")
df = spark.createDataFrame(pd.read_json(df_json))

# Traitement des données
df = df.filter(df.gender == 'female').select('first_name', 'last_name')

# Envoi du DataFrame traité aux tâches suivantes
task_instance.xcom_push(key="my_processed_data_frame", value=df.toPandas().to_json())
