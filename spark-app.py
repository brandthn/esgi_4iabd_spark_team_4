# spark-app.py
import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":
    arg1 = sys.argv[1] if len(sys.argv) > 1 else None
    arg2 = sys.argv[2] if len(sys.argv) > 2 else None

    spark = SparkSession.builder.appName("LocalSparkApp").getOrCreate()

    print(f"Hello Spark! arg1={arg1}, arg2={arg2}")

    data = [("Alice", 34), ("Bob", 36)]
    df = spark.createDataFrame(data, ["name", "age"])
    df.show()

    spark.stop()
