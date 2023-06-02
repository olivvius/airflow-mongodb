import requests
from pyspark.sql import SparkSession

# Création d'une session Spark
spark = SparkSession.builder.appName("GenerateData").getOrCreate()

# Création d'un DataFrame vide avec les colonnes attendues
df = spark.createDataFrame([], schema="gender string, first_name string, last_name string")

# Boucle pour effectuer 10 requêtes à l'API
for i in range(3):
    # Requête à l'API pour récupérer des données aléatoires
    response = requests.get("https://randomuser.me/api/").json()

    # Extraction des données intéressantes
    gender = response["results"][0]["gender"]
    first_name = response["results"][0]["name"]["first"]
    last_name = response["results"][0]["name"]["last"]

    # Ajout des données au DataFrame
    new_row = spark.createDataFrame([(gender, first_name, last_name)], schema=df.schema)
    df = df.union(new_row)

# Stockage du DataFrame en mémoire et envoi aux tâches suivantes
task_instance = context.get("task_instance")
task_instance.xcom_push(key="my_data_frame", value=df.toPandas().to_json())
