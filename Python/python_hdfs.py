from hdfs import InsecureClient
from pyspark.sql import SparkSession
from delta import *

hadoop_address = 'http://172.24.0.6:9870/'
client = InsecureClient(hadoop_address, user='root')
fichier = 'texte.txt'
client.makedirs('/projet')
hdfs = '/projet/' + fichier
client.download(hdfs, fichier, overwrite=True)
print('ok')

"""
#client.download(hdfs_path, fichier)
with open(fichier, 'r') as f:
    print("Contenu :")
    read = f.read()
    print(read)

spark = SparkSession.builder .appName("deltalake") .getOrCreate()
hdfs = 'hdfs://172.24.0.6:9000/projet/texte.txt'
df = spark.read.csv(hdfs, header=True, inferSchema=True)
print('ok spark')

delta = 'hdfs://172.24.0.6:9000/projet/delta_table'
df.write.format("delta").mode("overwrite").save(delta)
spark.stop()
print('ok delta')
"""
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