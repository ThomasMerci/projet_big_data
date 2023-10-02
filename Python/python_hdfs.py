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