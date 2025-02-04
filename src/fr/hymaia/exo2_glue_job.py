import sys
import time
from datetime import datetime
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def mesurer_temps(nom_fonction):
    """Décorateur pour mesurer le temps d'exécution"""
    def decorateur(func):
        def wrapper(*args, **kwargs):
            debut = time.time()
            resultat = func(*args, **kwargs)
            fin = time.time()
            duree = fin - debut
            print(f"Temps d'exécution {nom_fonction}: {duree:.2f} secondes")
            return resultat, duree
        return wrapper
    return decorateur

@mesurer_temps("Transformation Spark Native")
def transformer_donnees(df):
    """Transformation utilisant les fonctions natives Spark pour performance optimale"""
    return df.withColumn(
        "category_name",
        F.when(F.col("category") < 6, "food").otherwise("furniture")
    )

def main():
    spark = SparkSession.builder \
        .appName("TransformationCategories") \
        .getOrCreate()
        
    glueContext = GlueContext(spark.sparkContext)
    job = Job(glueContext)
    
    # Récupération paramètres avec chemin de sortie
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "input_path", "output_bucket"])
    job.init(args['JOB_NAME'], args)
    
    input_path = args['input_path']
    output_bucket = args['output_bucket']
    
    try:
        print(f"Lecture des données depuis : {input_path}")
        df = spark.read.option("header", "true") \
                      .option("inferSchema", "true") \
                      .csv(f"{input_path}/*.csv")
        
        print("Schéma des données d'entrée :")
        df.printSchema()
        nombre_lignes = df.count()
        print(f"Nombre de lignes à traiter : {nombre_lignes}")
        
        df_transforme, duree = transformer_donnees(df)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        chemin_sortie = f"s3://{output_bucket}/output/categories_transform_{timestamp}"
        
        # Sauvegarde résultats au format parquet
        print(f"Sauvegarde des résultats dans : {chemin_sortie}")
        df_transforme.write.mode("overwrite").parquet(chemin_sortie)
        
        print("\nRésumé de l'exécution:")
        print(f"- Nombre de lignes traitées: {nombre_lignes}")
        print(f"- Temps de traitement: {duree:.2f} secondes")
        print(f"- Vitesse moyenne: {nombre_lignes/duree:.2f} lignes/seconde")
        print(f"- Résultats sauvegardés dans: {chemin_sortie}")
        
    except Exception as e:
        print(f"Erreur lors de l'exécution : {str(e)}")
        raise e
    
    job.commit()

if __name__ == '__main__':
    main()