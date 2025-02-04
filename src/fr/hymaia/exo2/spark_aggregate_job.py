from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as f

def create_spark_session():
    return SparkSession.builder \
        .appName("aggregate_client_data") \
        .master("local[*]") \
        .getOrCreate()

def aggregate_by_department(df: DataFrame) -> DataFrame:
    """Agrège les clients par département"""
    return df.groupBy("departement") \
        .agg(f.count("*").alias("nb_people")) \
        .orderBy(f.col("nb_people").desc(), f.col("departement").asc())

def main():
    spark = create_spark_session()
    try:
        clean_df = spark.read.parquet("data/exo2/clean")
        
        # Agrégation
        result_df = aggregate_by_department(clean_df)
        
        # Écriture dans un seul fichier CSV avec coalesce(1)
        result_df.coalesce(1).write \
            .mode("overwrite") \
            .option("header", True) \
            .csv("data/exo2/aggregate")
            
    finally:
        spark.stop()

if __name__ == "__main__":
    main()