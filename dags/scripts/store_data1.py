import pymongo
import pandas as pd

# Récupération du DataFrame traité stocké en mémoire
task_instance = context.get("task_instance")
df_json = task_instance.xcom_pull(task_ids="process_data_task", key="my_processed_data_frame")
df = pd.read_json(df_json)

# Conversion du DataFrame en liste de dictionnaires
data = df.to_dict('records')

# Connexion à la base de données MongoDB
client = pymongo.MongoClient("mongodb://mongo:27017/")
db = client["mydatabase"]
collection = db["mycollection"]

# Stockage des données dans la base MongoDB
collection.insert_many(data)
