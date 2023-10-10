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
#import python_extract
#import python_ml
#import DataAnalysis
#import data_processing


#mise en place des fihciers dans hdfs 
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




#hdfs
hadoop_address = 'http://namenode:9870/'
client = InsecureClient(hadoop_address, user='root')

#csv
df_orders_csv = './projet/orders.csv'
df_bikes_csv = './projet/bikes.csv'
df_bikeshops_csv = './projet/bikeshops.csv'
with client.read(df_orders_csv, encoding='utf-8') as hdfs_file:
    df_orders_csv = pd.read_csv(hdfs_file)
with client.read(df_bikes_csv, encoding='utf-8') as hdfs_file:
    df_bikes_csv = pd.read_csv(hdfs_file)
with client.read(df_bikeshops_csv, encoding='utf-8') as hdfs_file:
    df_bikeshops_csv = pd.read_csv(hdfs_file)

#script python
#DataAnalysis.analyze_and_clean_data()
#data_processing.processing_and_save_data(df_orders_csv, df_bikes_csv, df_bikeshops_csv)

#join, data pour le ML
#df_ml, data_bikes = python_extract.extract(df_orders_csv, df_bikes_csv, df_bikeshops_csv)
#print("lecture")
#df_ml.show()
#data_bikes.show()


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


# delta lecture
csv_data.write.format("delta").mode("overwrite").save("/tmp/csv_table")
df_csv = spark.read.format("delta").load("/tmp/csv_table")
df_csv.show(10)


# hdfs lecture
fichier_hdfs = '/projet/bikes.csv'
with client.read(fichier_hdfs, encoding='utf-8') as hdfs_file:
    df = pd.read_csv(hdfs_file)
    print(df.head())
    print(df.describe())



print('fin')
