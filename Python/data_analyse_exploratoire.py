from pyspark.sql import SparkSession
import matplotlib.pyplot as plt

def analyse_exploratoire():
    # Initialiser SparkSession
    spark = (SparkSession
             .builder
             .appName("DataAnalyseExploratoire")
             .enableHiveSupport()
             .getOrCreate()
             )

    try:
        # Chargement des fichiers parquet
        joinedDF = spark.read.parquet("joined.parquet")

        # Conversion de la colonne "category1" en un objet Pandas DataFrame et tracé de l'histogramme
        pandas_df = joinedDF.select("category1").toPandas()
        plt.hist(pandas_df["category1"], bins=3, color='lightblue', edgecolor='black')

        # Personnalisation de l'histogramme
        plt.title('Distribution des données de la colonne "category1"')
        plt.xlabel('Valeurs')
        plt.ylabel('Fréquence')
        plt.grid(axis='y', alpha=0.75)
        plt.xticks(fontsize=10)
        plt.yticks(fontsize=10)

        # Affichage de l'histogramme
        plt.show()

        # Conversion de la colonne "category2" en un objet Pandas DataFrame et tracé de l'histogramme
        pandas_df = joinedDF.select("category2").toPandas()
        plt.hist(pandas_df["category2"], bins=3, color='lightblue', edgecolor='black')

        # Personnalisation de l'histogramme
        plt.title('Distribution des données de la colonne "category2"')
        plt.xlabel('Valeurs')
        plt.ylabel('Fréquence')
        plt.grid(axis='y', alpha= 0.75)
        plt.xticks(fontsize=5)
        plt.yticks(fontsize=10)

        # Affichage de l'histogramme
        plt.show()

        # Conversion de la colonne "frame" en un objet Pandas DataFrame et tracé de l'histogramme
        pandas_df = joinedDF.select("frame").toPandas()
        plt.hist(pandas_df["frame"], bins=3, color='lightblue', edgecolor='black')

        # Personnalisation de l'histogramme
        plt.title('Distribution des données de la colonne "frame"')
        plt.xlabel('Valeurs')
        plt.ylabel('Fréquence')
        plt.grid(axis='y', alpha= 0.75)
        plt.xticks(fontsize=10)
        plt.yticks(fontsize=10)

        # Affichage de l'histogramme
        plt.show()

    finally:
        # Stop SparkSession
        spark.stop()

# Appeler la fonction pour exécuter l'analyse exploratoire
analyse_exploratoire()
