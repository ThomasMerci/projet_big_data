import pyspark
from delta import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from hdfs import InsecureClient
#import python_extract


#hdfs
import os
from hdfs import InsecureClient
from pyspark.sql import SparkSession
from delta import *

hadoop_address = 'http://namenode:9870/' #namenode
client = InsecureClient(hadoop_address, user='root')
client.makedirs('/projet')
print(hadoop_address)

#orders
orders = './orders.csv'
if os.path.exists(orders):
    print(f"Le fichier {orders} existe.")
else:
    print(f"Le fichier {orders} n'existe pas")
    exit(1)
#vers hdfs
fichier_hdfs = '/projet/' + orders.split('/')[-1]
client.upload(fichier_hdfs,orders, overwrite=True)

#bikes
bikes = './bikes.csv'
if os.path.exists(bikes):
    print(f"Le fichier {bikes} existe.")
else:
    print(f"Le fichier {bikes} n'existe pas")
    exit(1)
#vers hdfs
fichier_hdfs = '/projet/' + bikes.split('/')[-1]
client.upload(fichier_hdfs,bikes, overwrite=True)

#bikeshops
bikeshops = './bikeshops.csv'
if os.path.exists(bikeshops):
    print(f"Le fichier {bikeshops} existe.")
else:
    print(f"Le fichier {bikeshops} n'existe pas")
    exit(1)
#vers hdfs
fichier_hdfs = '/projet/' + bikeshops.split('/')[-1]
client.upload(fichier_hdfs,bikeshops, overwrite=True)



###

#hdfs
hadoop_address = 'http://namenode:9870/'
client = InsecureClient(hadoop_address, user='root')

#csv
df_orders_csv = './projet/orders.csv'
df_bikes_csv = './projet/bikes.csv'
df_bikeshops_csv = './projet/bikesshops.csv'


#join, data pour le ML
#df_ml, data_bikes = python_extract.extract(df_orders_csv, df_bikes_csv, df_bikeshops_csv)




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







#hdfs
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
