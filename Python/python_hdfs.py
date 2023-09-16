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

#lecture
with open(fichier_local, 'r') as f:
    print("Contenu :")
    read = f.read()
    print(read)

print('debutspark')

#spark

spark = SparkSession.builder.appName("deltalake").getOrCreate()
"""
spark = SparkSession .builder .appName("deltalake") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
"""
spark.sparkContext.setLogLevel("INFO")
print(spark.sparkContext._conf.getAll())

df = spark.read.csv("hdfs://namenode:9000/projet/texte.csv", header=True, sep=";")
df.show()






"""
#deltalake
df_delta = 'hdfs://namenode:9000/projet/delta_table'
df.write.format("delta").mode("overwrite").save(df_delta)
df_verif = spark.read.format("delta").load(df_delta)
df_verif.show()

spark.stop()
print('OK Delta')


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