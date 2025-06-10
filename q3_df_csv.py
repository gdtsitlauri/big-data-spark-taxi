from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

spark = SparkSession.builder.appName("Q3_DF_CSV").getOrCreate()

tripdata_csv = "hdfs://hdfs-namenode:9000/data/yellow_tripdata_2024.csv"
zones_csv = "hdfs://hdfs-namenode:9000/data/taxi_zone_lookup.csv"

df = spark.read.option("header", True).option("inferSchema", True).csv(tripdata_csv)
zones = spark.read.option("header", True).csv(zones_csv)

zones_pickup = zones.withColumnRenamed("LocationID", "PULocationID") \
                    .withColumnRenamed("Borough", "Pickup_Borough")

zones_dropoff = zones.withColumnRenamed("LocationID", "DOLocationID") \
                     .withColumnRenamed("Borough", "Dropoff_Borough")

df_joined = df.join(zones_pickup, on="PULocationID", how="left") \
              .join(zones_dropoff, on="DOLocationID", how="left")

df_same = df_joined.filter(col("Pickup_Borough") == col("Dropoff_Borough"))

result = df_same.groupBy("Pickup_Borough").agg(count("*").alias("TotalTrips")) \
                .orderBy(col("TotalTrips").desc())

print("=== Results: Q3 using DataFrame over CSV ===")
result.show(truncate=False)

spark.stop()
