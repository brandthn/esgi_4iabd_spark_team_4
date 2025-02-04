from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
import time

def creer_spark_session():

    return (SparkSession.builder
            .appName("Spark Native Functions Optimisé")
            # Optimisation de la mémoire
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.shuffle.partitions", "200")
            .config("spark.memory.fraction", "0.8")
            # Optimisation du stockage
            .config("spark.sql.inMemoryColumnarStorage.compressed", "true")
            .config("spark.sql.files.maxPartitionBytes", "128mb")
            .getOrCreate())

def definir_schema():
    return StructType([
        StructField("id", IntegerType(), True),
        StructField("date", StringType(), True),
        StructField("category", IntegerType(), True),
        StructField("price", DoubleType(), True)
    ])

def collecter_metriques_performance(debut_temps, df_resultat, description=""):
    temps_fin = time.time()
    temps_execution = temps_fin - debut_temps
    nombre_enregistrements = df_resultat.cache().count()

    print(f"\n{'='*60}")
    print(f"Métriques de Performance - {description}")
    print(f"{'='*60}")
    print(f"Temps d'exécution: {temps_execution:.2f} secondes")
    print(f"Enregistrements traités: {nombre_enregistrements:,}")
    print(f"Temps/enregistrement: {(temps_execution/nombre_enregistrements)*1000:.3f} ms")
    
    print("\nPlan d'exécution optimisé:")
    df_resultat.explain(mode="cost")
    
    return {
        'temps_execution': temps_execution,
        'nombre_enregistrements': nombre_enregistrements,
        'temps_moyen': (temps_execution/nombre_enregistrements)*1000
    }

def premiere_partie(spark):
    debut = time.time()
    
    df = (spark.read
          .option("header", "true")
          .schema(definir_schema())
          .csv("src/resources/exo4/sell.csv"))
    
    nombre_partitions = max(df.rdd.getNumPartitions(), 
                          spark.sparkContext.defaultParallelism)
    df = df.repartition(nombre_partitions, "category")
    
    df_resultat = (df
        .withColumn("category_name",
            F.when(F.col("category").cast("int") < 6, "food")
            .otherwise("furniture"))
        .select("id", "date", "category", "price", "category_name"))
    
    metriques = collecter_metriques_performance(debut, df_resultat, "Première partie")
    
    print("\nAperçu des résultats (Première partie):")
    df_resultat.show(5)
    
    return df_resultat, metriques

def deuxieme_partie(df_premiere_partie):

    debut = time.time()
    
    df = (df_premiere_partie
          .withColumn("date", F.to_date("date"))
          .withColumn("price", F.col("price").cast("double")))
    
    df = df.repartition("category_name", "date")
    
    fenetre_journaliere = Window.partitionBy("category_name", "date")
    
    fenetre_30_jours = (Window.partitionBy("category_name")
                       .orderBy("date")
                       .rowsBetween(-30, 0))  # Utilisation de rowsBetween au lieu de rangeBetween
    
    df_final = (df
        .withColumn("total_price_per_category_per_day",
                   F.sum("price").over(fenetre_journaliere))
        .withColumn("total_price_per_category_per_day_last_30_days",
                   F.sum("price").over(fenetre_30_jours)))
    
    metriques = collecter_metriques_performance(debut, df_final, "Deuxième partie")
    
    print("\nAperçu des résultats finaux:")
    df_final.orderBy("date", "category_name").show(5)
    
    return df_final, metriques

def main():

    spark = creer_spark_session()
    
    try:
        print("\nExécution de la première partie...")
        df_premiere_partie, metriques_p1 = premiere_partie(spark)
        
        print("\nExécution de la deuxième partie...")
        df_final, metriques_p2 = deuxieme_partie(df_premiere_partie)
        
        # Sauvegarde des métriques
        with open('metriques_no_udf.txt', 'w') as f:
            f.write("=== Première partie ===\n")
            for key, value in metriques_p1.items():
                f.write(f"{key}: {value}\n")
            f.write("\n=== Deuxième partie ===\n")
            for key, value in metriques_p2.items():
                f.write(f"{key}: {value}\n")
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()