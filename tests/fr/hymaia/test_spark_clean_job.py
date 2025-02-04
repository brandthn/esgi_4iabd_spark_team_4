import pytest
from tests.fr.hymaia.spark_test_case import spark

from src.fr.hymaia.exo2.spark_clean_job import (
    filter_adult_clients,
    join_with_cities,
    get_department,
    add_department
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


CLIENT_SCHEMA = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("zip", StringType(), True)
])

CITY_SCHEMA = StructType([
    StructField("zip", StringType(), True),
    StructField("city", StringType(), True)
])

def test_filter_adult_clients():
    """Test du filtrage des clients majeurs"""
    df = spark.createDataFrame([
        ("Toto", 17, "75001"),
        ("Momo", 18, "75002"),
        ("Lolo", 25, "75003")
    ], CLIENT_SCHEMA)
    
    assert df.schema == CLIENT_SCHEMA
    result_df = filter_adult_clients(df)
    assert result_df.count() == 2
    assert all(row.age >= 18 for row in result_df.collect())

def test_join_with_cities():
    """Test de la jointure avec les villes"""
    clients_df = spark.createDataFrame([
        ("Toto", 20, "75001")
    ], CLIENT_SCHEMA)
    
    cities_df = spark.createDataFrame([
        ("75001", "Paris"),
        ("75001", "Paris-Autre")
    ], CITY_SCHEMA)
    
    assert clients_df.schema == CLIENT_SCHEMA
    assert cities_df.schema == CITY_SCHEMA
    
    result_df = join_with_cities(clients_df, cities_df)
    result = result_df.collect()
    assert len(result) == 1
    assert result[0].city == "Paris"

def test_get_department():
    """Test : extraction du département"""
    assert get_department("75001") == "75"
    assert get_department("13001") == "13"
    assert get_department("20180") == "2A"
    assert get_department("20200") == "2B"

def test_get_department_error():
    """Test : cas d'erreur pour l'extraction du département"""
    assert get_department("") is None
    assert get_department("ABC") is None

def test_add_department():
    """Test : ajout de la colonne département"""
    df = spark.createDataFrame([
        ("Toto", 20, "75001"),
        ("Momo", 25, "20180"),
        ("Lolo", 30, "20200")
    ], CLIENT_SCHEMA)
    
    assert df.schema == CLIENT_SCHEMA
    result_df = add_department(df)
    
    departments = {row.zip: row.departement for row in result_df.collect()}
    assert departments["75001"] == "75"
    assert departments["20180"] == "2A"
    assert departments["20200"] == "2B"

def test_filter_adult_clients_error():
    """Test du cas d'erreur : colonne age manquante"""
    # DataFrame sans la colonne 'age'
    df = spark.createDataFrame([
        ("Toto", "75001")
    ], ["name", "zip"])
    
    # La fonction devrait lèverr une exception
    with pytest.raises(Exception) as exc_info:
        filter_adult_clients(df)

    # Vérification du message d'erreur
    assert "age" in str(exc_info.value)