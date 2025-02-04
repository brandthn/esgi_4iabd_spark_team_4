from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
import time

def creer_spark_session():
    return (SparkSession.builder
            .appName("Python UDF Optimisé")
            .config("spark.python.worker.memory", "1g")
            .config("spark.python.worker.reuse", "true")
            .config("spark.sql.inMemoryColumnarStorage.compressed", "true")
            .getOrCreate())

def definir_schema():

    return StructType([
        StructField("id", IntegerType(), True),
        StructField("date", StringType(), True),
        StructField("category", IntegerType(), True),
        StructField("price", DoubleType(), True)
    ])

@udf(returnType=StringType())
def obtenir_nom_categorie(categorie):

    return "food" if categorie < 6 else "furniture"

def collecter_metriques_performance(debut_temps, df_resultat, nom_test="Python UDF"):

    temps_fin = time.time()
    temps_execution = temps_fin - debut_temps
    
    nombre_enregistrements = df_resultat.cache().count()
    
    print(f"\n{'='*60}")
    print(f"Métriques de Performance Détaillées - {nom_test}")
    print(f"{'='*60}")
    print(f"Temps d'exécution total: {temps_execution:.2f} secondes")
    print(f"Nombre d'enregistrements traités: {nombre_enregistrements:,}")
    print(f"Temps moyen par enregistrement: {(temps_execution/nombre_enregistrements)*1000:.3f} ms")
    
    print("\nPlan d'exécution:")
    df_resultat.explain(extended=True)
    
    print(f"{'='*60}\n")
    
    return {
        'temps_execution': temps_execution,
        'nombre_enregistrements': nombre_enregistrements,
        'temps_moyen_par_enregistrement': temps_execution/nombre_enregistrements
    }

def traiter_donnees(spark):

    debut = time.time()
    
    df = (spark.read
          .option("header", "true")
          .schema(definir_schema())
          .csv("src/resources/exo4/sell.csv"))
    
    df_resultat = (df
                  .withColumn("category_name", 
                             obtenir_nom_categorie(col("category")))
                  # Sélection explicite des colonnes pour optimiser la mémoire
                  .select("id", "date", "category", "price", "category_name"))
    
    # Collecte métriques
    metriques = collecter_metriques_performance(debut, df_resultat)
    
    #  résultats
    print("Aperçu des résultats:")
    df_resultat.show(5, truncate=False)
    
    return df_resultat, metriques

def main():

    spark = creer_spark_session()
    
    try:
        df_resultat, metriques = traiter_donnees(spark)
        
        # Sauvegarde des metriques
        with open('metriques_python_udf.txt', 'w') as f:
            for key, value in metriques.items():
                f.write(f"{key}: {value}\n")
                
    finally:
        spark.stop()

if __name__ == "__main__":
    main()