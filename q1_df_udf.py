from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, avg
from pyspark.sql.types import IntegerType
from datetime import datetime

def extract_hour_ts(pickup_timestamp):
    try:
        return pickup_timestamp.hour
    except Exception:
        return None

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Q1_DF_UDF").getOrCreate()

    udf_extract_hour_ts = udf(extract_hour_ts, IntegerType())

    input_path = "hdfs://hdfs-namenode:9000/data/yellow_tripdata_2015.csv"

    df_csv = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv(input_path)

    df_filtered = df_csv.filter(
        (col("pickup_latitude") != 0.0) &
        (col("pickup_longitude") != 0.0)
    )

    df_with_hour = df_filtered.withColumn(
        "hour",
        udf_extract_hour_ts(col("tpep_pickup_datetime"))
    )

    result_df = df_with_hour.groupBy("hour").agg(
        avg("pickup_latitude").alias("avg_latitude"),
        avg("pickup_longitude").alias("avg_longitude")
    ).orderBy("hour")

    result_df.show(24, truncate=False)

    spark.stop()