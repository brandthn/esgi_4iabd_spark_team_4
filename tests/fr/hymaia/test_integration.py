from tests.fr.hymaia.spark_test_case import spark
from src.fr.hymaia.exo2.spark_clean_job import filter_adult_clients, join_with_cities, add_department
from src.fr.hymaia.exo2.spark_aggregate_job import aggregate_by_department

def test_full_pipeline():
    """Test d'intégration du pipeline complet"""
    clients_df = spark.createDataFrame([
        ("Toto", 17, "75001"),
        ("Momo", 20, "75001"),
        ("Lolo", 25, "75002"),
        ("Dodo", 30, "13001")
    ], ["name", "age", "zip"])
    
    cities_df = spark.createDataFrame([
        ("75001", "Paris"),
        ("75002", "Paris"),
        ("13001", "Marseille")
    ], ["zip", "city"])
    
    clean_df = (clients_df
               .transform(filter_adult_clients)
               .transform(lambda df: join_with_cities(df, cities_df))
               .transform(add_department))
    
    final_df = aggregate_by_department(clean_df)
    
    assert clean_df.count() == 3
    
    result = final_df.collect()
    assert len(result) == 2
    assert result[0].departement == "75"
    assert result[0].nb_people == 2
    assert result[1].departement == "13"
    assert result[1].nb_people == 1