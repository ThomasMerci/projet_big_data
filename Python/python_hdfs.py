import os
from hdfs import InsecureClient
from pyspark.sql import SparkSession
from delta import *

hadoop_address = 'http://namenode:9870/' #namenode
client = InsecureClient(hadoop_address, user='root')
print(hadoop_address)

#vérification des fichiers
fichier_local = './texte.csv'
if os.path.exists(fichier_local):
    print(f"Le fichier {fichier_local} existe.")
else:
    print(f"Le fichier {fichier_local} n'existe pas")
    exit(1)

#vers hdfs
client.makedirs('/projet')
fichier_hdfs = '/projet/' + fichier_local.split('/')[-1]
client.upload(fichier_hdfs,fichier_local, overwrite=True)
print('ok')

#lecture
with open(fichier_local, 'r') as f:
    print("Contenu :")
    read = f.read()
    print(read)

#spark
spark = SparkSession.builder.appName("deltalake").getOrCreate()
spark.sparkContext.setLogLevel("INFO")

hdfs_path = f'hdfs://namenode:9870{fichier_hdfs}'
df = spark.read.option("delimiter", ";").csv(hdfs_path, header=True, inferSchema=True)
print('ok spark')

#deltalake
delta_path = 'hdfs://namenode:9870/projet/delta_table'
df.write.format("delta").mode("overwrite").save(delta_path)

spark.stop()
print('ok delta')


"""
df.selectExpr("split(_c0, ' ') as texte").show(4,False)

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# créer une table
data = spark.range(0, 5)
data.write.format("delta").save("/tmp/delta-table")

# lire 
df = spark.read.format("delta").load("/tmp/delta-table")
df.show()

# écrire
data = spark.range(5, 10)
data.write.format("delta").mode("overwrite").save("/tmp/delta-table")
"""