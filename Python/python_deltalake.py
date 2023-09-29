import os
from hdfs import InsecureClient
from pyspark.sql import SparkSession

print("début")

spark = SparkSession.builder.appName("deltalake") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

data = spark.range(0, 5)

(data
   .write
   .format("delta")
   .mode("overwrite") 
   .save("/tmp/delta-table"))

df = (spark
        .read
        .format("delta")
        .load("/tmp/delta-table")
        .orderBy("id")
      )

df.show()

