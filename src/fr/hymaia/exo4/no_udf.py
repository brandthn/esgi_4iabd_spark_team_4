from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import time

def creer_spark_session():
    return SparkSession.builder \
        .appName("Spark Functions") \
        .getOrCreate()

def premiere_partie(spark):
    # Début chronométrage
    debut = time.time()
    
    df = spark.read.csv("src/resources/exo4/sell.csv", header=True)
    
    # Fonctions natives de Spark
    df_resultat = df.withColumn(
        "category_name",
        F.when(F.col("category").cast("int") < 6, "food").otherwise("furniture")
    )
    
    # Mesure le temps
    nombre = df_resultat.count()
    fin = time.time()
    
    # métriques
    print(f"\n{'='*50}")
    print("Métriques de performance (Première partie - Sans UDF):")
    print(f"Temps de traitement: {fin - debut:.2f} secondes")
    print(f"Nombre d'enregistrements traités: {nombre}")
    print(f"Temps moyen par enregistrement: {(fin - debut)/nombre:.5f} secondes")
    print(f"{'='*50}\n")
    
    # résultats
    print("Résultats de la première partie:")
    df_resultat.show(5)
    
    return df_resultat

def deuxieme_partie(df_premiere_partie):
    """Deuxième partie de l'exercice : window functions"""
    debut = time.time()
    
    # Conversion des types pour window functions
    df = df_premiere_partie.withColumn("date", F.to_date("date")) \
                          .withColumn("price", F.col("price").cast("double"))
    
    # Définition fenêtre jour
    fenetre_journaliere = Window.partitionBy("category_name", "date")
    
    # Calcul total par catégorie et par jour
    df_avec_total_jour = df.withColumn(
        "total_price_per_category_per_day",
        F.sum("price").over(fenetre_journaliere)
    )
    
    # Définition de la fenêtre pour les 30 derniers jours
    fenetre_30_jours = Window.partitionBy("category_name") \
                            .orderBy("date") \
                            .rowsBetween(-30, 0)
    
    # Ajout du total sur les 30 derniers jours
    df_final = df_avec_total_jour.withColumn(
        "total_price_per_category_per_day_last_30_days",
        F.sum("price").over(fenetre_30_jours)
    )
    
    # Mesure le temps
    nombre = df_final.count()
    fin = time.time()
    
    # métriques
    print(f"\n{'='*50}")
    print("Métriques de performance (Deuxième partie - Window Functions):")
    print(f"Temps de traitement: {fin - debut:.2f} secondes")
    print(f"Nombre d'enregistrements traités: {nombre}")
    print(f"Temps moyen par enregistrement: {(fin - debut)/nombre:.5f} secondes")
    print(f"{'='*50}\n")
    
    # Résultats
    print("Résultats de la deuxième partie: (5 exemples)")
    df_final.orderBy("date", "category_name").show(5)
    
    return df_final

def main():
    spark = creer_spark_session()
    
    # Exécution - première partie
    print("\nExécution de la première partie (Spark Function - Sans UDF)")
    df_premiere_partie = premiere_partie(spark)
    
    # Exécution - deuxième partie
    print("\nExécution de la deuxième partie (Window Functions)")
    df_final = deuxieme_partie(df_premiere_partie)
    
    spark.stop()

if __name__ == "__main__":
    main()