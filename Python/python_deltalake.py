import pyspark
from delta import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

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
schema = StructType([
    StructField("id", IntegerType(), True),       # Changez IntegerType() en StringType() si l'ID est une chaîne de caractères
    StructField("model", StringType(), True),
    StructField("category1", StringType(), True),
    StructField("category2", StringType(), True),
    StructField("frame", StringType(), True),
    StructField("price", DoubleType(), True)      # Changez DoubleType() en StringType() si le prix est une chaîne de caractères
])

# Chargez les données depuis le fichier CSV avec le schéma explicite
data_csv = "./texte.csv"
csv_data = spark.read.option("header", "true").option("delimiter", ";").schema(schema).csv(data_csv)

# Appliquez na.drop() au DataFrame
csv_data = csv_data.na.drop()

# Affichez les 10 premières lignes
csv_data.show(10)

# Enregistrez le DataFrame nettoyé au format Delta
csv_data.write.format("delta").mode("overwrite").save("/tmp/csv_table")

# Chargez le DataFrame nettoyé depuis Delta
df_csv = spark.read.format("delta").load("/tmp/csv_table")
df_csv.show(10)
