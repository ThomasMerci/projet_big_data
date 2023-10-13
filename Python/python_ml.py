from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, lit, date_add, to_date, dayofmonth, dayofweek, datediff
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.functions import dayofmonth, dayofweek, to_date, datediff, lit, date_add, coalesce, col
from pyspark.sql.types import DoubleType, FloatType, IntegerType
from pyspark.sql import DataFrame, functions as Funct
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline

def rl_recette(df):
    df_ML = df.groupBy("order_date").agg(
        Funct.sum("quantity").alias("total_quantity"),
        Funct.sum("price").alias("total_price"))

    df_ML = df_ML.withColumn("order_date", to_date(df_ML["order_date"], "M/d/yyyy"))
    df_ML = df_ML.withColumn("jour", dayofmonth(df_ML["order_date"]))
    df_ML = df_ML.withColumn("jour_semaine", dayofweek(df_ML["order_date"]))
    min_date = df_ML.selectExpr("min(order_date)").first()[0]
    df_ML = df_ML.withColumn("nb_jour", datediff(df_ML["order_date"], lit(min_date)))
    df_ML = df_ML.fillna(0, subset=["jour"])

    df_ML = df_ML.withColumn("total_price", col("total_price").cast(DoubleType()))
    df_ML = df_ML.withColumn("nb_jour", col("nb_jour").cast(DoubleType()))
    df_ML = df_ML.withColumn("jour", col("jour").cast(DoubleType()))
    df_ML = df_ML.withColumn("jour_semaine", col("jour_semaine").cast(DoubleType()))
    df_ML = df_ML.withColumn("total_quantity", col("total_quantity").cast(DoubleType()))

    df_ML1 = df_ML.select("nb_jour", "total_price", "total_quantity") 
    assembler = VectorAssembler(inputCols=["nb_jour"], outputCol="features")

    # rl total_price
    lr_price = LinearRegression(featuresCol="features", labelCol="total_price", predictionCol="pred_total_price")
    pipeline_price = Pipeline(stages=[assembler, lr_price])
    train_data_price, test_data_price = df_ML1.randomSplit([0.8, 0.2], seed=100)

    model_price = pipeline_price.fit(train_data_price)
    predictions_price = model_price.transform(test_data_price)
    predictions_price.select("nb_jour", "total_price", "pred_total_price").show()
    
    # model total_quantity
    lr_quantity = LinearRegression(featuresCol="features", labelCol="total_quantity", predictionCol="pred_total_quantity")
    pipeline_quantity = Pipeline(stages=[assembler, lr_quantity])
    train_data_quantity, test_data_quantity = df_ML1.randomSplit([0.8, 0.2], seed=100)

    model_quantity = pipeline_quantity.fit(train_data_quantity)
    predictions_quantity = model_quantity.transform(test_data_quantity)

    predictions_quantity.select("nb_jour", "total_quantity", "pred_total_quantity").show()
    jours = 730
    df_pred = spark.range(1, jours + 1).withColumnRenamed("id", "nb_jour")
    df_pred = model_price.transform(df_pred)
    df_resultat = df_pred.select("nb_jour", "pred_total_price")

    # Date
    min_date_orders = df.selectExpr("max(order_date)").first()[0]
    df_resultat = df_resultat.select("nb_jour", "pred_total_price")
    df_resultat = df_resultat.withColumn("nb_jour", col("nb_jour").cast(IntegerType()))
    df_resultat = df_resultat.withColumn("order_date", date_add(lit(min_date_orders), df_resultat["nb_jour"]))

    # total_price
    data_pred_price = df_ML.join(df_resultat, "order_date", "full")
    data_pred_price = data_pred_price.withColumn("recette", coalesce(data_pred_price["pred_total_price"], data_pred_price["total_price"]))

    df_pred_quantity = spark.range(1, jours + 1).withColumnRenamed("id", "nb_jour")
    df_pred_quantity = model_quantity.transform(df_pred_quantity)
    df_resultat_quantity = df_pred_quantity.select("nb_jour", "pred_total_quantity")

    # date total_quantity
    df_resultat_quantity = df_resultat_quantity.select("nb_jour", "pred_total_quantity")
    df_resultat_quantity = df_resultat_quantity.withColumn("nb_jour", col("nb_jour").cast(IntegerType()))
    df_resultat_quantity = df_resultat_quantity.withColumn("order_date", date_add(lit(min_date_orders), df_resultat_quantity["nb_jour"]))

    data_pred_quantity = df_ML.join(df_resultat_quantity, "order_date", "full")
    data_pred_quantity = data_pred_quantity.withColumn("quantity", coalesce(data_pred_quantity["pred_total_quantity"], data_pred_quantity["total_quantity"]))

    # fusion
    data_pred = data_pred_price.join(data_pred_quantity, "order_date", "full")
    data_pred = data_pred.select("order_date", "pred_total_price", "recette", "pred_total_quantity", "quantity")

    return data_pred

