from tests.fr.hymaia.spark_test_case import spark
from src.fr.hymaia.exo2.spark_aggregate_job import aggregate_by_department
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

CLEANED_DATA_SCHEMA = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("zip", StringType(), True),
    StructField("city", StringType(), True),
    StructField("departement", StringType(), True)
])

def test_aggregate_by_department():
    """Test : agrégation par département"""
    df = spark.createDataFrame([
        ("Toto", 20, "75001", "Paris", "75"),
        ("Momo", 25, "75002", "Paris", "75"),
        ("Lolo", 30, "13001", "Marseille", "13")
    ], CLEANED_DATA_SCHEMA)
    
    assert df.schema == CLEANED_DATA_SCHEMA
    result_df = aggregate_by_department(df)
    result = result_df.collect()
    
    assert len(result) == 2
    assert result[0].departement == "75"
    assert result[0].nb_people == 2
    assert result[1].departement == "13"
    assert result[1].nb_people == 1