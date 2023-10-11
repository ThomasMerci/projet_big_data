from pyspark.sql.functions import dayofmonth, dayofweek, to_date, datediff, lit, date_add, coalesce, col, StructType
from pyspark.sql.types import DoubleType, FloatType, IntegerType
from pyspark.sql import DataFrame, functions as Funct
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark
from pyspark.sql import SparkSession

from pyspark.sql.functions import dayofmonth, dayofweek, to_date, datediff, lit, date_add, coalesce, col
from pyspark.sql.types import DoubleType, FloatType, IntegerType
from pyspark.sql import DataFrame, functions as Funct
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline

def rl_recette(df_ML, df):
    spark = SparkSession.builder.appName("data").enableHiveSupport().getOrCreate()
    """
    col_ML = ["order_date", "nb_jour", "quantity", "jour", "jour_semaine", "price"]
    df_ML = df_ml.select(col_ML)

    df_ML = df_ML.groupBy("order_date").agg(Funct.sum("quantity").alias("total_quantity"),
        Funct.sum("price").alias("total_price"))

    df_ML = df_ML.withColumn("order_date", to_date(df_ML["order_date"], "M/d/yyyy"))
    df_ML = df_ML.withColumn("jour", dayofmonth(df_ML["order_date"]))
    df_ML = df_ML.withColumn("jour_semaine", dayofweek(df_ML["order_date"]))
    min_date = df_ML.selectExpr("min(order_date)").first()[0]
    df_ML = df_ML.withColumn("nb_jour", datediff(df_ML["order_date"], lit(min_date)))
    df_ML = df_ML.fillna(0, subset=["jour"])
    """
    #cast
    df_ML = df_ML.withColumn("total_price", col("total_price").cast(DoubleType()))
    df_ML = df_ML.withColumn("nb_jour", col("nb_jour").cast(DoubleType()))

    df_ML = df_ML.select("nb_jour", "total_price")
    assembler = VectorAssembler(inputCols=["nb_jour"], outputCol="features")

    #regression
    lr = LinearRegression(featuresCol="features", labelCol="total_price", predictionCol="pred_total_price")
    pipeline = Pipeline(stages=[assembler, lr])
    train_data, test_data = df_ML.randomSplit([0.8, 0.2], seed=100)
    model = pipeline.fit(train_data)
    predictions = model.transform(test_data)

    jours = 730
    df_pred = spark.range(1, jours + 1).withColumnRenamed("id", "nb_jour")
    df_pred = model.transform(df_pred)
    df_resultat = df_pred.select("nb_jour", "pred_total_price")

    min_date = df.selectExpr("max(order_date)").first()[0]
    df_resultat = df_resultat.select("nb_jour", "pred_total_price")
    df_resultat = df_resultat.withColumn("nb_jour", col("nb_jour").cast(IntegerType())) 
    df_resultat = df_resultat.withColumn("order_date", date_add(lit(min_date), df_resultat["nb_jour"]))
    df_resultat = df_resultat.select("order_date", "pred_total_price")

    data_pred = df.join(df_resultat, "order_date", "full")
    data_pred = data_pred.withColumn("recette", coalesce(data_pred["pred_total_price"], data_pred["price"]))
    
    return data_pred