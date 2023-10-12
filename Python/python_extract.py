from pyspark.sql.functions import dayofmonth, dayofweek, to_date, datediff, lit, date_add, coalesce, col, StructType
from pyspark.sql.types import DoubleType, FloatType, IntegerType
from pyspark.sql import DataFrame, functions as Funct
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark
from pyspark.sql import SparkSession

def extract(df_orders, df_bikes, df_bikeshops, df_customers):

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
            StructField("product_id", IntegerType(), True),
            StructField("model", StringType(), True),
            StructField("category1", StringType(), True),
            StructField("category2", StringType(), True),
            StructField("frame", StringType(), True),
            StructField("price", DoubleType(), True)])
    df_bikes = (spark.read.format("csv").option("inferSchema", "false").option("delimiter", ";").option("header", "true").schema(bikesSchema).load(df_bikes))

    bikeshopSchema = StructType([
            StructField("customer_id", IntegerType(), True),
            StructField("bikeshop_name", StringType(), True),
            StructField("bikeshop_city", StringType(), True),
            StructField("bikeshop_state", StringType(), True),
            StructField("latitude", StringType(), True),
            StructField("longitude", StringType(), True)])
    df_bikeshops = (spark.read.format("csv").option("inferSchema", "false").option("delimiter", ";").option("header", "true").schema(bikeshopSchema).load(df_bikeshops))
    
    customerSchema = StructType([
            StructField("customer_id", IntegerType(), True),
            StructField("Prefix", StringType(), True),
            StructField("FirstName", StringType(), True),
            StructField("LastName", StringType(), True),
            StructField("BirthDate", StringType(), True),
            StructField("Gender", StringType(), True),
            StructField("EmailAddress", StringType(), True),
            StructField("AnnualIncome", StringType(), True),
            StructField("TotalChildren", StringType(), True),
            StructField("EducationLevel", StringType(), True),
            StructField("Occupation", StringType(), True),
            StructField("HomeOwner", StringType(), True)])
    df_customers = (spark.read.format("csv").option("inferSchema", "false").option("delimiter", ",").option("header", "true").schema(customerSchema).load(df_customers))

    df_orders = df_orders.withColumn("order_date", to_date(df_orders["order_date"], "M/d/yyyy"))
    df_orders = df_orders.withColumn("jour", dayofmonth(df_orders["order_date"]))
    df_orders = df_orders.withColumn("jour_semaine", dayofweek(df_orders["order_date"]))

    min_date = df_orders.selectExpr("min(order_date)").first()[0]
    df_orders = df_orders.withColumn("nb_jour", datediff(df_orders["order_date"], lit(min_date)))
    df_orders = df_orders.fillna(0, subset=["jour"])

    #join
    df_1 = df_orders.join(df_bikes, on="product_id", how="inner")
    df = df_1.join(df_bikeshops, on="customer_id", how="inner")
    df = df_1.join(df_customers, on="customer_id", how="inner")

    df = df.withColumn("quantity", col("quantity").cast(DoubleType()))
    df = df.withColumn("price", col("price").cast(DoubleType()))

    #partie analyse
    # Supprimer les lignes dupliquer
    df = df.dropDuplicates()

    # enlever les valeurs aberrantes
    for col_name in df.columns:
        try:
            numeric_col = df[col_name].cast(DoubleType())
            if numeric_col is not None:
                stats = df.select(avg(col_name), stddev(col_name)).first()
                mean = stats[0]
                std = stats[1]

                if mean is not None and std is not None:
                    threshold = 3 * std + mean
                    df = df.filter(df[col_name] <= threshold)
        except:
            pass

    #data pour le ml
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



