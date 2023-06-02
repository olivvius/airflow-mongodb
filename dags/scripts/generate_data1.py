import requests
import pandas as pd

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
task_instance = context.get("task_instance")
task_instance.xcom_push(key="my_data_frame", value=df.to_json())
