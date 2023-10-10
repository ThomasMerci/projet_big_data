from pyspark.sql.functions import dayofmonth, dayofweek, to_date, datediff, lit, date_add, coalesce, col
from pyspark.sql.types import DoubleType, FloatType, IntegerType
from pyspark.sql import DataFrame, functions as Funct
import pyspark
from pyspark.sql import SparkSession

def extract(df_orders, df_bikes, df_bikeshops):

    spark = SparkSession.builder.appName("data").enableHiveSupport().getOrCreate()
    df_orders = spark.createDataFrame(df_orders)
    df_bikes = spark.createDataFrame(df_bikes)
    df_bikeshops = spark.createDataFrame(df_bikeshops)

    
    df_orders = df_orders.withColumnRenamed("product.id", "bike")
    df_orders = df_orders.withColumnRenamed("order.id", "order_id")
    df_orders = df_orders.withColumnRenamed("order.date", "order_date")
    df_orders = df_orders.withColumnRenamed("customer.id", "bikeshop")

    df_orders = df_orders.withColumn("order_date", to_date(df_orders["order_date"], "M/d/yyyy"))
    df_orders = df_orders.withColumn("jour", dayofmonth(df_orders["order_date"]))
    df_orders = df_orders.withColumn("jour_semaine", dayofweek(df_orders["order_date"]))

    min_date = df_orders.selectExpr("min(order_date)").first()[0]
    df_orders = df_orders.withColumn("nb_jour", datediff(df_orders["order_date"], lit(min_date)))
    df_orders = df_orders.fillna(0, subset=["jour"])

    #bikes.csv
    df_bikes = df_bikes.withColumnRenamed("bike.id", "bike")

    #bikeshop.csv
    df_bikeshops = df_bikeshops.withColumnRenamed("bikeshop.id", "bikeshop")

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

    return df_ML, df



