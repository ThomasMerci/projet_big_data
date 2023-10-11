import pyspark
from delta import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from hdfs import InsecureClient
import os
from hdfs import InsecureClient
from pyspark.sql import SparkSession
from delta import *
import pyarrow.hdfs as hdfs
import pandas as pd
from delta import DeltaTable
import python_extract
#import python_ml
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
            print(f"Le fichier {local} n'existe pas localement. Téléchargement depuis HDFS...")
            fichier_hdfs = hdfs + local.split('/')[-1]
            client.upload(fichier_hdfs, local, overwrite=True)
        else:
            print(f"Le fichier {local} existe localement. Utilisation du fichier local.")

    except Exception as e:
        print(f"Erreur lors du traitement de {local}: {e}")

upload_hdfs('./orders.csv', '/projet/', client)
upload_hdfs('./bikes.csv', '/projet/', client)
upload_hdfs('./bikeshops.csv', '/projet/', client)

#hdfs lecture
dfs = {}
hdfs_csvs = ['/projet/orders.csv', '/projet/bikes.csv', '/projet/bikeshops.csv']
for hdfs_csv in hdfs_csvs:
    local = './' + hdfs_csv.split('/')[-1]
    if not os.path.exists(local):
        client.download(hdfs_csv, local)
        print(f"Téléchargement de {local} depuis HDFS.")
    else:
        print(f"Le fichier {local} existe localement. Utilisation du fichier local.")

    with client.read(hdfs_csv, encoding='utf-8') as hdfs_data:
        dfs[local] = pd.read_csv(hdfs_data)

for key in dfs.keys():
    print(f"fichier : {key}")

df_ml, data_bikes = python_extract.extract('./orders.csv', './bikes.csv', './bikeshops.csv')
df_ml.show(10)

#script python
#DataAnalysis.analyze_and_clean_data()
#data_processing.processing_and_save_data(df_orders_csv, df_bikes_csv, df_bikeshops_csv)

#spark
builder = pyspark.sql.SparkSession.builder.appName("deltalake") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# table
data = spark.range(0, 5)
data.write.format("delta").mode("overwrite").save("/tmp/data_table")
df = spark.read.format("delta").load("/tmp/data_table")
df.show()

# csv
schema = StructType([StructField("id", IntegerType(), True), 
    StructField("model", StringType(), True),
    StructField("category1", StringType(), True),
    StructField("category2", StringType(), True),
    StructField("frame", StringType(), True),
    StructField("price", DoubleType(), True) ])
data_csv = "./texte.csv"
csv_data = spark.read.option("header", "true").option("delimiter", ";").schema(schema).csv(data_csv)
csv_data = csv_data.na.drop()
csv_data.show(10)


# delta 
csv_data.write.format("delta").mode("overwrite").save("/tmp/csv_table")
df_csv = spark.read.format("delta").load("/tmp/csv_table")
df_csv.show(10)
data_bikes.show(10)

#enregistrement df_ml dans deltalake
df_ml.write.format("delta").mode("overwrite").save("/tmp/df_ml")
data_bikes.write.format("delta").mode("overwrite").save("/tmp/data_bikes")
df_ml_delta = spark.read.format("delta").load("/tmp/df_ml")
data_bikes_delta = spark.read.format("delta").load("/tmp/data_bikes")
data_bikes_delta.show(10)
df_ml.show(10)

#lister les dossiers delta
import os
listes = os.listdir("/tmp/")
for file in listes:
    chemin = os.path.join("/tmp/", file)
    if os.path.isfile(chemin):
        print(f"Fichier : {file}")
    elif os.path.isdir(chemin):
        print(f"Dossier : {file}")



print('fin')

for local_csv in dfs:
    os.remove(local_csv)