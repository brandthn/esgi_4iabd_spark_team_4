from pyspark.sql import SparkSession
from pyspark.sql.column import Column, _to_java_column, _to_seq
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
import time

def creer_spark_session():
    """
    Création d'une session Spark optimisée pour l'utilisation des UDF Scala.
    Les configurations ajoutées visent à optimiser l'interaction Java/Scala.
    """
    return (SparkSession.builder
            .appName("Scala UDF Optimisé")
            # Configuration pour le jar Scala
            .config('spark.jars', 'src/resources/exo4/udf.jar')
            # Optimisation de la mémoire pour les opérations JVM
            .config('spark.memory.offHeap.enabled', 'true')
            .config('spark.memory.offHeap.size', '1g')
            # Optimisation de la sérialisation
            .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
            # Compression des données
            .config('spark.sql.inMemoryColumnarStorage.compressed', 'true')
            .getOrCreate())

def definir_schema():
    """
    Définition explicite du schéma pour optimiser la lecture des données.
    Cette approche évite l'inférence de schéma coûteuse.
    """
    return StructType([
        StructField("id", IntegerType(), True),
        StructField("date", StringType(), True),
        StructField("category", IntegerType(), True),
        StructField("price", DoubleType(), True)
    ])

def addCategoryName(col):
    if not hasattr(addCategoryName, "udf_instance"):
        sc = spark.sparkContext
        addCategoryName.udf_instance = sc._jvm.fr.hymaia.sparkfordev.udf.Exo4.addCategoryNameCol()
    
    return Column(addCategoryName.udf_instance.apply(_to_seq(spark.sparkContext, [col], _to_java_column)))

def collecter_metriques_performance(debut_temps, df_resultat, nom_test="Scala UDF"):
    temps_fin = time.time()
    temps_execution = temps_fin - debut_temps
    
    nombre_enregistrements = df_resultat.cache().count()
    
    print(f"\n{'='*60}")
    print(f"Métriques de Performance Détaillées - {nom_test}")
    print(f"{'='*60}")
    print(f"Temps d'exécution total: {temps_execution:.2f} secondes")
    print(f"Nombre d'enregistrements traités: {nombre_enregistrements:,}")
    print(f"Temps moyen par enregistrement: {(temps_execution/nombre_enregistrements)*1000:.3f} ms")
    
    print("\nPlan d'exécution détaillé:")
    df_resultat.explain(mode="cost")
    
    return {
        'temps_execution': temps_execution,
        'nombre_enregistrements': nombre_enregistrements,
        'temps_moyen_par_enregistrement': (temps_execution/nombre_enregistrements)*1000
    }

def traiter_donnees(spark_session):

    debut = time.time()
    
    df = (spark_session.read
          .option("header", "true")
          .schema(definir_schema())
          .csv("src/resources/exo4/sell.csv"))
    
    nombre_partitions = max(df.rdd.getNumPartitions(), 
                          spark_session.sparkContext.defaultParallelism)
    df = df.repartition(nombre_partitions)
    
    df_resultat = (df
                  .withColumn("category", col("category").cast("int"))
                  .withColumn("category_name", addCategoryName(col("category")))
                  # Sélection explicite pour optimiser la mémoire
                  .select("id", "date", "category", "price", "category_name"))
    
    # Collecte des métriques
    metriques = collecter_metriques_performance(debut, df_resultat)
    
    print("\nAperçu des résultats:")
    df_resultat.show(5, truncate=False)
    
    return df_resultat, metriques

def main():
    global spark
    spark = creer_spark_session()
    
    try:
        df_resultat, metriques = traiter_donnees(spark)
        
        # Sauvegarde des métriques
        with open('metriques_scala_udf.txt', 'w') as f:
            for key, value in metriques.items():
                f.write(f"{key}: {value}\n")
                
    finally:
        if 'spark' in globals():
            spark.stop()

if __name__ == "__main__":
    main()