import pyspark
from delta import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from hdfs import InsecureClient
import os
from hdfs import InsecureClient
from pyspark.sql import SparkSession
from delta import *
import pandas as pd
from delta import DeltaTable
from prometheus_client import *
import time
import psutil
import python_extract
import python_ml
#import data_analysis
#import data_processing

#mise en place des fihciers dans hdfs
hadoop_address = 'http://namenode:9870/' #namenode
client = InsecureClient(hadoop_address, user='root')

client.makedirs('/projet')
print(hadoop_address)

#hdfs enregistrement
def upload_hdfs(local, hdfs, client):
    try:
        if not os.path.exists(local):
            print(f"Le fichier {local} n'existe pas localement, téléchargement")
            fichier_hdfs = hdfs + local.split('/')[-1]
            client.upload(fichier_hdfs, local, overwrite=True)

        else:
            print(f"Le fichier {local} existe localement")

    except Exception as e:
        print(f"Erreur lors du traitement de {local}: {e}")

#upload_hdfs('./orders.csv', '/projet/', client)
#upload_hdfs('./bikes.csv', '/projet/', client)
#upload_hdfs('./bikeshops.csv', '/projet/', client)

#hdfs lecture
dfs = {}
hdfs_csvs = ['/projet/orders.csv', '/projet/bikes.csv', '/projet/bikeshops.csv', '/projet/customers.csv']
for hdfs_csv in hdfs_csvs:
    local = './' + hdfs_csv.split('/')[-1]
    if not os.path.exists(local):
        client.download(hdfs_csv, local)
        print(f"Téléchargement de {local} depuis HDFS.")
    else:
        print(f"Le fichier {local} existe localement")

    with client.read(hdfs_csv, encoding='utf-8') as hdfs_data:
        dfs[local] = pd.read_csv(hdfs_data)

data_bikes = python_extract.extract('./orders.csv', './bikes.csv', './bikeshops.csv', './customers.csv')

# Obtenez le chemin absolu vers le volume monté dans le conteneur
volume_projet = '/data/'
if not os.path.exists(volume_projet):
    os.makedirs(volume_projet)
data_bikes_coal = data_bikes.coalesce(1)    
data_bikes_coal.write.csv(os.path.join(volume_projet, 'data_bikes.csv'), header=True, mode='overwrite', sep=';')

# hdfs
upload_hdfs('/data/data_bikes.csv', '/projet/data_bikes.csv', client)
print("fin hdfs")

#hdfs_path_df_ml = '/projet/df_ml'
#hdfs_path_data_bikes = '/projet/data_bikes'
#df_ml.write.csv(hdfs_path_df_ml, header=True, mode='overwrite', sep=';')
#data_bikes.write.csv(hdfs_path_data_bikes, header=True, mode='overwrite', sep=';')


for key in dfs.keys():
    print(f"fichier : {key}")
#script python
#DataAnalysis.analyze_and_clean_data()
#data_processing.processing_and_save_data(df_orders_csv, df_bikes_csv, df_bikeshops_csv)

#spark
builder = pyspark.sql.SparkSession.builder.appName("deltalake") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
data = spark.range(0, 5)
data.write.format("delta").mode("overwrite").save("/projet/data_table")

#enregistrement/lecture df_ml dans deltalake
data_bikes.write.format("delta").mode("overwrite").save("/projet/data_bikes")
data_bikes_delta = spark.read.format("delta").load("/projet/data_bikes")

#ml
data_ml = python_ml.rl_recette(data_bikes)
data_ml.write.format("delta").mode("overwrite").save("/projet/data_ml")
data_ml_delta = spark.read.format("delta").load("/projet/data_ml")
data_ml_delta.show(10)

#lister les dossiers delta
import os
listes = os.listdir("/projet/")
for file in listes:
    chemin = os.path.join("/projet/", file)
    if os.path.isfile(chemin):
        print(f"Fichier : {file}")
    elif os.path.isdir(chemin):
        print(f"Dossier : {file}")

#df_hdfs = spark.read.format("delta").load("hdfs://namenode:8020/projet/data_table")
#df_hdfs.show()

# Analyse
print("Calcul des corrélations de Pearson pour chaque paire de colonne : \n")
# Calculate Pearson correlation
num_col = []

# Convert string columns to double when possible
for column in data_bikes.columns:
    try:
        if int(data_bikes.select(column).first()[0]):
            num_col += [column]
    except:
        pass 

# Calculer la corrélation de pearson pour chaque pair de colonne
correlations = {}
for i in range(len(num_col)):
    for j in range(i+1, len(num_col)):
        col_i = num_col[i]
        col_j = num_col[j]
        corr_val = data_bikes.select(corr(col_i, col_j)).collect()[0][0]
        correlations[(col_i, col_j)] = corr_val

# Afficher le résultat
for cols, corr_val in correlations.items():
    print("La corrélation de Pearson entre {} et {} est : {}".format(cols[0], cols[1], corr_val))

# Métriques prometheus

# Créer une métrique personnalisée
REQUEST_TIME = Summary('request_processing_seconds', 'Time spent processing request')
CPU_USAGE = Gauge('cpu_usage_percent', 'Pourcentage utilisation CPU')
MEMORY_USAGE = Gauge('memory_usage_bytes', 'Utilisation de la mémoire')


# Fonction pour traiter une demande et mesurer le temps
def process_request():
    start_time = time.time()
    # Ici, vous effectuez le traitement de votre demande
    # Par exemple, vous pouvez simuler le traitement en attendant pendant quelques secondes
    time.sleep(2)
    end_time = time.time()
    REQUEST_TIME.observe(end_time - start_time)
    # Mesurer l'utilisation du CPU et de la mémoire
    cpu_percent = psutil.cpu_percent(interval=None)  # Mesurer l'utilisation du CPU
    memory_usage = psutil.virtual_memory().used  # Mesurer l'utilisation de la mémoire

    # Mettre à jour les métriques de CPU et de mémoire
    CPU_USAGE.set(cpu_percent)
    MEMORY_USAGE.set(memory_usage)

print('fin')
for local_csv in dfs:
    os.remove(local_csv)

if __name__ == '__main__':
    # Démarrez le serveur HTTP pour exposer les métriques
    start_http_server(8004)
    while True:
        # Traitement d'une demande
        process_request()