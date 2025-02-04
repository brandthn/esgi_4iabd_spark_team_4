from pyspark.sql import SparkSession
from pyspark.sql.column import Column, _to_java_column, _to_seq
import time

# Variable globale pour la session Spark
spark = None

def creer_spark_session():
    """Crée et retourne une session Spark avec le jar Scala configuré"""
    return SparkSession.builder \
        .appName("Scala UDF") \
        .config('spark.jars', 'src/resources/exo4/udf.jar') \
        .getOrCreate()

def addCategoryName(col):
    # on récupère le SparkContext
    sc = spark.sparkContext
    # Via sc._jvm on peut accéder à des fonctions Scala
    add_category_name_udf = sc._jvm.fr.hymaia.sparkfordev.udf.Exo4.addCategoryNameCol()
    # On retourne un objet colonne avec l'application de notre udf Scala
    return Column(add_category_name_udf.apply(_to_seq(sc, [col], _to_java_column)))

def traiter_donnees(spark_session):

    debut = time.time()
    
    df = spark_session.read.csv("src/resources/exo4/sell.csv", header=True)
    
    # UDF
    df_resultat = df.withColumn("category_name", 
                               addCategoryName(df.category.cast("int")))
    
    # Mesure le temps
    nombre = df_resultat.count()
    fin = time.time()
    
    # métriques
    print(f"\n{'='*50}")
    print("Métriques de performance (UDF Scala):")
    print(f"Temps de traitement: {fin - debut:.2f} secondes")
    print(f"Nombre d'enregistrements traités: {nombre}")
    print(f"Temps moyen par enregistrement: {(fin - debut)/nombre:.5f} secondes")
    print(f"{'='*50}\n")
    
    #  résultats
    print("Résultats (5 Exemples):")
    df_resultat.show(5)
    
    return df_resultat

def main():
    global spark
    spark = creer_spark_session()
    traiter_donnees(spark)
    spark.stop()

if __name__ == "__main__":
    main()