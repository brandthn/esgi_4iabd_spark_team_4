import sys
import time
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.column import Column, _to_java_column, _to_seq

def mesurer_temps(nom_fonction):
    """Décorateur pour mesurer le temps d'exécution"""
    def decorateur(func):
        def wrapper(*args, **kwargs):
            debut = time.time()
            resultat = func(*args, **kwargs)
            fin = time.time()
            print(f"Temps d'exécution {nom_fonction}: {fin - debut:.2f} secondes")
            return resultat
        return wrapper
    return decorateur

@mesurer_temps("Approche sans UDF")
def transformation_sans_udf(df):
    """Transformation utilisant les fonctions natives Spark"""
    return df.withColumn(
        "category_name",
        F.when(F.col("category") < 6, "food").otherwise("furniture")
    )

@mesurer_temps("Approche avec Python UDF")
def transformation_python_udf(df):
    """Transformation utilisant Python UDF"""
    @F.udf(StringType())
    def categorie_to_name(categorie):
        return "food" if int(categorie) < 6 else "furniture"
    
    return df.withColumn(
        "category_name",
        categorie_to_name(F.col("category"))
    )

@mesurer_temps("Approche avec Scala UDF")
def transformation_scala_udf(spark, df):
    """Transformation utilisant Scala UDF"""
    sc = spark.sparkContext
    try:
        add_category_name_udf = sc._jvm.fr.hymaia.sparkfordev.udf.Exo4.addCategoryNameCol()
        return df.withColumn(
            "category_name",
            F.expr("fr_hymaia_sparkfordev_udf_Exo4.addCategoryNameCol(category)")
        )
    except Exception as e:
        print(f"Erreur lors de l'exécution de l'UDF Scala : {str(e)}")
        return df

def executer_et_comparer_approches(spark, chemin_donnees):
    """Exécute et compare les différentes approches"""
    print(f"Tentative de lecture depuis : {chemin_donnees}")
    
    try:
        df = spark.read.option("header", "true") \
                      .option("inferSchema", "true") \
                      .csv(f"{chemin_donnees}/*.csv")
        
        print("Schéma du DataFrame :")
        df.printSchema()
        print(f"Nombre total de lignes : {df.count()}")
        
        # Sans UDF
        df_sans_udf = transformation_sans_udf(df)
        print("\nRésultat sans UDF (exemple) :")
        df_sans_udf.show(5)
        
        # Avec Python UDF
        df_python_udf = transformation_python_udf(df)
        print("\nRésultat avec Python UDF (exemple) :")
        df_python_udf.show(5)
        
        # Avec Scala UDF
        df_scala_udf = transformation_scala_udf(spark, df)
        print("\nRésultat avec Scala UDF (exemple) :")
        df_scala_udf.show(5)
        
    except Exception as e:
        print(f"Erreur lors de l'exécution : {str(e)}")
        raise e

def main():
    spark = SparkSession.builder \
        .appName("ComparaisonUDF") \
        .config('spark.jars', 's3://esgi-spark-pf-bucket/jars/udf.jar') \
        .getOrCreate()
        
    glueContext = GlueContext(spark.sparkContext)
    job = Job(glueContext)
    
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "input_path"])
    job.init(args['JOB_NAME'], args)
    
    input_path = args['input_path']
    
    executer_et_comparer_approches(spark, input_path)
    
    job.commit()

if __name__ == '__main__':
    main()