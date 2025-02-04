import pyspark.sql.functions as f
from pyspark.sql import SparkSession

def wordcount(df, col_name):
    return df.withColumn('word', f.explode(f.split(f.col(col_name), ' '))) \
        .groupBy('word') \
        .count()

def main():
    spark = SparkSession.builder \
    .appName("wordcount") \
    .master("local[*]") \
    .getOrCreate()

    df = spark.read.option("header", True).csv("src/resources/exo1/data.csv")
    wordcount_df = wordcount(df, "text")

    wordcount_df.write.partitionBy("count").mode("overwrite").parquet("data/exo1/output")


if __name__ == "__main__":
    main()


