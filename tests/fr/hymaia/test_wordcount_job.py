from tests.fr.hymaia.spark_test_case import spark
from src.fr.hymaia.exo1.main import wordcount
from pyspark.sql.types import StructType, StructField, StringType

def test_wordcount():
    """Test de la fonction de comptage de mots"""
    # schéma
    schema = StructType([
        StructField("text", StringType(), True)
    ])
    
    # DataFrame de test
    df = spark.createDataFrame([("hello hello world",)], schema)
    
    # Vérification schéma
    assert df.schema == schema
    
    result_df = wordcount(df, "text")
    
    # Conversion en liste et tri
    result_list = [(row["word"], row["count"]) for row in result_df.collect()]
    expected = [("hello", 2), ("world", 1)]
    
    # Vérification
    assert sorted(result_list) == sorted(expected)