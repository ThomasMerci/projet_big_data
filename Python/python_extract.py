from pyspark.sql.functions import dayofmonth, dayofweek, to_date, datediff, lit, date_add, coalesce, col, StructType
from pyspark.sql.types import DoubleType, FloatType, IntegerType
from pyspark.sql import DataFrame, functions as Funct
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark
from pyspark.sql import SparkSession

def extract(df_orders, df_bikes, df_bikeshops):

    spark = SparkSession.builder.appName("data").enableHiveSupport().getOrCreate()

    ordersSchema = StructType([
            StructField("id", IntegerType(), True),
            StructField("order_id", IntegerType(), True),
            StructField("order_line", IntegerType(), True),
            StructField("order_date", StringType(), True),
            StructField("customer_id", IntegerType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("quantity", IntegerType(), True)])
    df_orders = (spark.read.format("csv").option("inferSchema", "false").option("delimiter", ";").option("header", "true").schema(ordersSchema).load(df_orders))
    df_orders = df_orders.withColumn("order_date", to_date(df_orders["order_date"], "M/d/yyyy"))

    bikesSchema = StructType([
            StructField("bike_id", IntegerType(), True),
            StructField("model", StringType(), True),
            StructField("category1", StringType(), True),
            StructField("category2", StringType(), True),
            StructField("frame", StringType(), True),
            StructField("price", DoubleType(), True)])
    df_bikes = (spark.read.format("csv").option("inferSchema", "false").option("delimiter", ";").option("header", "true").schema(bikesSchema).load(df_bikes))

    bikeshopSchema = StructType([
            StructField("bikeshop_id", IntegerType(), True),
            StructField("bikeshop_name", StringType(), True),
            StructField("bikeshop_city", StringType(), True),
            StructField("bikeshop_state", StringType(), True),
            StructField("latitude", StringType(), True),
            StructField("longitude", StringType(), True)])
    df_bikeshops = (spark.read.format("csv").option("inferSchema", "false").option("delimiter", ";").option("header", "true").schema(bikeshopSchema).load(df_bikeshops))

    #df_orders = spark.createDataFrame(df_orders)
    #df_bikes = spark.createDataFrame(df_bikes)
    #df_bikeshops = spark.createDataFrame(df_bikeshops)


    df_orders = df_orders.withColumnRenamed("product_id", "bike")
    df_orders = df_orders.withColumnRenamed("order_id", "order_id")
    df_orders = df_orders.withColumnRenamed("order_date", "order_date")
    df_orders = df_orders.withColumnRenamed("customer_id", "bikeshop")

    df_orders = df_orders.withColumn("order_date", to_date(df_orders["order_date"], "M/d/yyyy"))
    df_orders = df_orders.withColumn("jour", dayofmonth(df_orders["order_date"]))
    df_orders = df_orders.withColumn("jour_semaine", dayofweek(df_orders["order_date"]))

    min_date = df_orders.selectExpr("min(order_date)").first()[0]
    df_orders = df_orders.withColumn("nb_jour", datediff(df_orders["order_date"], lit(min_date)))
    df_orders = df_orders.fillna(0, subset=["jour"])

    #bikes.csv
    df_bikes = df_bikes.withColumnRenamed("bike_id", "bike")

    #bikeshop.csv
    df_bikeshops = df_bikeshops.withColumnRenamed("bikeshop_id", "bikeshop")

    #join
    df_1 = df_orders.join(df_bikes, on="bike", how="inner")
    df = df_1.join(df_bikeshops, on="bikeshop", how="inner")

    df = df.withColumn("quantity", col("quantity").cast(DoubleType()))
    df = df.withColumn("price", col("price").cast(DoubleType()))

    df_ML = df.groupBy("order_date").agg(
        Funct.sum("quantity").alias("total_quantity"),
        Funct.sum("price").alias("total_price"))

    df_ML = df_ML.withColumn("order_date", to_date(df_ML["order_date"], "M/d/yyyy"))
    df_ML = df_ML.withColumn("jour", dayofmonth(df_ML["order_date"]))
    df_ML = df_ML.withColumn("jour_semaine", dayofweek(df_ML["order_date"]))
    min_date = df_ML.selectExpr("min(order_date)").first()[0]
    df_ML = df_ML.withColumn("nb_jour", datediff(df_ML["order_date"], lit(min_date)))
    df_ML = df_ML.fillna(0, subset=["jour"])

    df.show
    return df_ML, df



