from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType

def analyze_and_clean_data():
    # Initialiser SparkSession
    spark = (SparkSession
             .builder
             .appName("DataAnalysis")
             .enableHiveSupport()
             .getOrCreate()
             )

    try:
        # Charger les fichiers parquets précedemment fourni par le processing_and_save_data()
        ordersDF = spark.read.parquet("orders.parquet")
        bikesDF = spark.read.parquet("bikes.parquet")
        bikeshopDF = spark.read.parquet("bikeshops.parquet")

        # Statistique global
        descriptionOrder = ordersDF.describe().toPandas().transpose()
        descriptionBike = bikesDF.describe().toPandas().transpose()
        descriptionBikeshop = bikeshopDF.describe().toPandas().transpose()

        print("Description statistique pour orders : \n")
        print(descriptionOrder)
        print("Description statistique pour bikes : \n")
        print(descriptionBike)
        print("Description statistique pour bikeshops : \n")
        print(descriptionBikeshop)

        # Supprimer les lignes dupliquer
        ordersDF = ordersDF.dropDuplicates()
        bikesDF = bikesDF.dropDuplicates()
        bikeshopDF = bikeshopDF.dropDuplicates()

        # Afficher les lignes avec des valeurs manquantes
        print("Colonne numérique avec des valeurs manquantes pour orders : \n")
        ordersDF.where(col("order_id").isNull() | col("order_line").isNull() | col("order_date").isNull() | col("customer_id").isNull() | col("product_id").isNull() | col("quantity").isNull()).show()

        print("Colonne numérique avec des valeurs manquantes pour bikes : \n")
        bikesDF.where(col("price").isNull()).show()
        print("Colonne non numérique avec des valeurs manquantes pour bikes : \n")
        bikesDF.where(col("model").isNull() | col("category1").isNull() | col("category2").isNull() | col("frame").isNull()).show()

        print("Colonne numérique avec des valeurs manquantes pour bikeshop : \n")
        bikeshopDF.where(col("latitude").isNull() | col("longitude").isNull()).show()
        print("Colonne non numérique avec des valeurs manquantes pour bikeshop : \n")
        bikeshopDF.where(col("bikeshop_name").isNull() | col("bikeshop_city").isNull() | col("bikeshop_state").isNull()).show()

        # Jointure de Order et Bike sur product_id et bike_id
        joinedOrderBikeDF = ordersDF.join(bikesDF, ordersDF["product_id"] == bikesDF["bike_id"], "inner")
        joinedOrderBikeDF = joinedOrderBikeDF.drop("bike_id")
        joinedOrderBikeDF = joinedOrderBikeDF.orderBy(asc("order_id"))

        # Jointure de Order et Bikeshop sur customer_id et bikeshop_id
        joinedOrderBikeshopDF = ordersDF.join(bikeshopDF, ordersDF["customer_id"] == bikeshopDF["bikeshop_id"], "inner")
        joinedOrderBikeshopDF = joinedOrderBikeshopDF.drop("bikeshop_id")
        joinedOrderBikeshopDF = joinedOrderBikeshopDF.orderBy(asc("order_id"))

        # Specifier des alias avant la jointure global
        joinedOrderBikeDF = joinedOrderBikeDF.alias("bike")
        joinedOrderBikeshopDF = joinedOrderBikeshopDF.alias("bikeshop")

        # Jointure global
        joinedDF = joinedOrderBikeDF.join(joinedOrderBikeshopDF, col("bike.id") == col("bikeshop.id"), "inner")

        # Supprimer les colonnes en doublon suite à la jointure
        joinedDF = joinedDF.drop(col("bikeshop.id"))
        joinedDF = joinedDF.drop(col("bikeshop.order_id"))
        joinedDF = joinedDF.drop(col("bikeshop.order_line"))
        joinedDF = joinedDF.drop(col("bikeshop.order_date"))
        joinedDF = joinedDF.drop(col("bikeshop.customer_id"))
        joinedDF = joinedDF.drop(col("bikeshop.product_id"))
        joinedDF = joinedDF.drop(col("bikeshop.quantity"))
        joinedDF = joinedDF.orderBy(asc("order_id"))

        # Affichage du resultat
        print("Dataframe résultant de la jointure entre Order, Bikes et Bikeshop : \n")

        # Boucle pour enlever les valeurs aberrantes de chaque colonne
        for col_name in joinedDF.columns:
            try:
                # Tentez de convertir la colonne en DoubleType
                numeric_col = joinedDF[col_name].cast(DoubleType())

                # Si la conversion réussit sans erreur, la colonne est numérique
                if numeric_col is not None:
                    # Calcul des statistiques pour la colonne
                    stats = joinedDF.select(avg(col_name), stddev(col_name)).first()
                    mean = stats[0]
                    std = stats[1]

                    if mean is not None and std is not None:
                        # Calcul du seuil pour déterminer les valeurs aberrantes
                        threshold = 3 * std + mean
                        # Suppression des valeurs aberrantes pour la colonne
                        joinedDF = joinedDF.filter(joinedDF[col_name] <= threshold)
            except:
                # Si la conversion génère une erreur, la colonne n'est pas numérique
                pass

        # Affichage du DataFrame sans les valeurs aberrantes
        joinedDF.show()

        # Calculate Pearson correlation
        num_col = []

        # Convert string columns to double when possible
        for column in joinedDF.columns:
            try:
                if int(joinedDF.select(column).first()[0]):
                    num_col += [column]
            except:
                pass

        # Calculer la corrélation de pearson pour chaque pair de colonne
        correlations = {}
        for i in range(len(num_col)):
            for j in range(i+1, len(num_col)):
                col_i = num_col[i]
                col_j = num_col[j]
                corr_val = joinedDF.select(corr(col_i, col_j)).collect()[0][0]
                correlations[(col_i, col_j)] = corr_val

        # Afficher le résultat
        for cols, corr_val in correlations.items():
            print("La corrélation de Pearson entre {} et {} est : {}".format(cols[0], cols[1], corr_val))

        # Enregistrer le résultat dans un fichier parquet
        joinedDF.write.mode("overwrite").parquet("joined.parquet")

    finally:
        # Stop SparkSession
        spark.stop()

# Appel de la fonction
analyze_and_clean_data()
