from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as f
from pyspark.sql.types import StringType

def create_spark_session():
    """Crée une session Spark"""
    return SparkSession.builder \
        .appName("clean_client_data") \
        .master("local[*]") \
        .getOrCreate()

def filter_adult_clients(df: DataFrame) -> DataFrame:
    """Filtrage pour garder que les clients majeurs"""
    return df.filter(f.col("age") >= 18)

def join_with_cities(clients_df: DataFrame, cities_df: DataFrame) -> DataFrame:
    """
    Joint les données clients avec les villes.
    Prend la première ville pour chaque code postal pour éviter la multiplication des lignes.
    """
    # On groupe d'abord les villes par code postal et on prend la première ville
    cities_unique = cities_df \
        .groupBy("zip") \
        .agg(f.first("city").alias("city"))
    
    # Puis ici on fait la jointure
    return clients_df.join(
        cities_unique,
        "zip",
        "left"
    )

def get_department(zip_code: str) -> str:
    """Extrait le département du code postal"""
    if not zip_code:
        return None
    
    try:
        zip_int = int(zip_code)
        if 20000 <= zip_int <= 20190:
            return "2A"
        elif 20191 <= zip_int <= 20999:
            return "2B"
        return zip_code[:2]
    except ValueError:
        return None

def add_department(df: DataFrame) -> DataFrame:
    """Ajoute colonne département"""
    get_department_udf = f.udf(get_department, StringType())
    return df.withColumn("departement", get_department_udf(f.col("zip")))

def main():
    spark = create_spark_session()
    try:
        clients_df = spark.read \
            .option("header", True) \
            .csv("src/resources/exo2/clients_bdd.csv")
        
        cities_df = spark.read \
            .option("header", True) \
            .csv("src/resources/exo2/city_zipcode.csv")
        
        result_df = (clients_df
                    .transform(filter_adult_clients)
                    .transform(lambda df: join_with_cities(df, cities_df))
                    .transform(add_department))
        

        result_df.write \
            .mode("overwrite") \
            .parquet("data/exo2/clean")
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()