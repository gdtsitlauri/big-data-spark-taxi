from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, avg

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Q1_DF").getOrCreate()

    input_path = "hdfs://hdfs-namenode:9000/data/yellow_tripdata_2015.csv"

    df_csv = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv(input_path)

    df_filtered = df_csv.filter(
        (col("pickup_latitude") != 0.0) &
        (col("pickup_longitude") != 0.0)
    )

    df_with_hour = df_filtered.withColumn("hour", hour(col("tpep_pickup_datetime")))

    result_df = df_with_hour.groupBy("hour").agg(
        avg("pickup_latitude").alias("avg_latitude"),
        avg("pickup_longitude").alias("avg_longitude")
    ).orderBy("hour")

    result_df.show(24, truncate=False)

    spark.stop()