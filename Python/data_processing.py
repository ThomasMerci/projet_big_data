from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def processing_and_save_data():
    # Initialiser SparkSession
    spark = (SparkSession
             .builder
             .appName("DataPreprocessing")
             .enableHiveSupport()
             .getOrCreate()
             )

    try:
        # Definir le schéma pour "orders.csv"  et le lire dans un dataframe
        ordersSchema = StructType([
            StructField("id", IntegerType(), True),
            StructField("order_id", IntegerType(), True),
            StructField("order_line", IntegerType(), True),
            StructField("order_date", StringType(), True),
            StructField("customer_id", IntegerType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("quantity", IntegerType(), True)
        ])

        ordersDF = (spark.read
                    .format("csv")
                    .option("inferSchema", "true")
                    .option("delimiter", ";")
                    .option("header", "true")
                    .schema(ordersSchema)
                    .load("/home/zafifomendrahagabriello/Téléchargements/BigData/Python/Data/orders.csv"))

        # Convertir "order_date" en date
        ordersDF = ordersDF.withColumn("order_date", to_date(ordersDF["order_date"], "M/d/yyyy"))

        # Definir le schéma pour "bikes.csv"  et le lire dans un dataframe
        bikesSchema = StructType([
            StructField("bike_id", IntegerType(), True),
            StructField("model", StringType(), True),
            StructField("category1", StringType(), True),
            StructField("category2", StringType(), True),
            StructField("frame", StringType(), True),
            StructField("price", DoubleType(), True)
        ])

        bikesDF = (spark.read
                   .format("csv")
                   .option("inferSchema", "true")
                   .option("delimiter", ";")
                   .option("header", "true")
                   .schema(bikesSchema)
                   .load("/home/zafifomendrahagabriello/Téléchargements/BigData/Python/Data/bikes.csv"))

        # Definir le schéma pour "bikeshops.csv"  et le lire dans un dataframe
        bikeshopSchema = StructType([
            StructField("bikeshop_id", IntegerType(), True),
            StructField("bikeshop_name", StringType(), True),
            StructField("bikeshop_city", StringType(), True),
            StructField("bikeshop_state", StringType(), True),
            StructField("latitude", StringType(), True),
            StructField("longitude", StringType(), True)
        ])

        bikeshopDF = (spark.read
                      .format("csv")
                      .option("inferSchema", "true")
                      .option("delimiter", ";")
                      .option("header", "true")
                      .schema(bikeshopSchema)
                      .load("/home/zafifomendrahagabriello/Téléchargements/BigData/Python/Data/bikeshops.csv"))

        # Remplacer les virgules par des points dans les colonnes latitude et longitude et puis les convertir en DoubleType
        bikeshopDF = bikeshopDF.withColumn("latitude", regexp_replace("latitude", ",", ".").cast(DoubleType()))
        bikeshopDF = bikeshopDF.withColumn("longitude", regexp_replace("longitude", ",", ".").cast(DoubleType()))

        # Enregistrer en fichier parquet
        ordersDF.write.mode("overwrite").parquet("orders.parquet")
        bikesDF.write.mode("overwrite").parquet("bikes.parquet")
        bikeshopDF.write.mode("overwrite").parquet("bikeshops.parquet")

    finally:
        spark.stop()

# Appel de la fonction
processing_and_save_data()
