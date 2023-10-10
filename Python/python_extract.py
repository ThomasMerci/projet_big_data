


def extract():
    #extract
    fichier = "/FileStore/tables/orders.csv"
    infer_schema = "false"
    first_row_is_header = "True"
    delimiter = ";"

    df_orders = spark.read.format("csv") .option("inferSchema", infer_schema) \
    .option("header", first_row_is_header) \
    .option("sep", delimiter) \
    .load(fichier)
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
    fichier = "/FileStore/tables/bikes.csv"
    infer_schema = "false"
    first_row_is_header = "True"
    delimiter = ";"

    df_bikes = spark.read.format("csv") .option("inferSchema", infer_schema) \
    .option("header", first_row_is_header) \
    .option("sep", delimiter) \
    .load(fichier)
    df_bikes = df_bikes.withColumnRenamed("bike.id", "bike")

    #bikeshop.csv
    fichier = "/FileStore/tables/bikeshops.csv"
    infer_schema = "false"
    first_row_is_header = "True"
    delimiter = ";"

    df_bikeshops = spark.read.format("csv") .option("inferSchema", infer_schema) \
    .option("header", first_row_is_header) \
    .option("sep", delimiter) \
    .load(fichier)
    df_bikeshops = df_bikeshops.withColumnRenamed("bikeshop.id", "bikeshop")


    #join
    df_1 = df_orders.join(df_bikes, on="bike", how="inner")
    df = df_1.join(df_bikeshops, on="bikeshop", how="inner")

    df = df.withColumn("quantity", col("quantity").cast(DoubleType()))
    df = df.withColumn("price", col("price").cast(DoubleType()))

    col_ML = ["order_date", "nb_jour", "quantity", "jour", "jour_semaine", "price"]
    df_ML = df_1.select(col_ML)

    df_ML = df_ML.groupBy("order_date").agg(
        Funct.sum("quantity").alias("total_quantity"),
        Funct.sum("price").alias("total_price"))

    df_ML = df_ML.withColumn("order_date", to_date(df_ML["order_date"], "M/d/yyyy"))
    df_ML = df_ML.withColumn("jour", dayofmonth(df_ML["order_date"]))
    df_ML = df_ML.withColumn("jour_semaine", dayofweek(df_ML["order_date"]))
    min_date = df_ML.selectExpr("min(order_date)").first()[0]
    df_ML = df_ML.withColumn("nb_jour", datediff(df_ML["order_date"], lit(min_date)))
    df_ML = df_ML.fillna(0, subset=["jour"])

    def return df_ML

