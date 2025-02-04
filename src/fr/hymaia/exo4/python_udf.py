from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import time

def creer_spark_session():
    return SparkSession.builder \
        .appName("Exemple Python UDF") \
        .getOrCreate()

def obtenir_nom_categorie(categorie):
    if categorie < 6:
        return "food"
    return "furniture"

def traiter_donnees(spark):

    # Début chronométrage
    debut = time.time()
    
    df = spark.read.csv("src/resources/exo4/sell.csv", header=True)
    
    # UDF
    nom_categorie_udf = udf(obtenir_nom_categorie, StringType())
    
    # Application de la transformation
    df_resultat = df.withColumn("category_name", 
                               nom_categorie_udf(df.category.cast("int")))
    
    # Mesure le temps
    nombre = df_resultat.count()
    fin = time.time()
    
    # Métriques
    print(f"\n{'='*50}")
    print("Métriques de performance (UDF Python):")
    print(f"Temps de traitement: {fin - debut:.2f} secondes")
    print(f"Nombre d'enregistrements traités: {nombre}")
    print(f"Temps moyen par enregistrement: {(fin - debut)/nombre:.5f} secondes")
    print(f"{'='*50}\n")
    
    # Résultats
    print("Résultats (5 Exemples):")
    df_resultat.show(5)
    
    return df_resultat

def main():
    spark = creer_spark_session()
    traiter_donnees(spark)
    spark.stop()

if __name__ == "__main__":
    main()