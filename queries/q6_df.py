from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col

spark = SparkSession.builder \
    .appName("Q6_DF") \
    .getOrCreate()

trips = spark.read.parquet("hdfs://hdfs-namenode:9000/user/gtsitlaouri/data/parquet/yellow_tripdata_2024")
zones = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv("hdfs://hdfs-namenode:9000/data/taxi_zone_lookup.csv")

zones = zones.withColumnRenamed("LocationID", "PULocationID")

joined = trips.join(zones, on="PULocationID")

grouped = joined.groupBy("Borough").agg(
    sum("fare_amount").alias("Fare ($)"),
    sum("tip_amount").alias("Tips ($)"),
    sum("tolls_amount").alias("Tolls ($)"),
    sum("extra").alias("Extras ($)"),
    sum("mta_tax").alias("MTA Tax ($)"),
    sum("congestion_surcharge").alias("Congestion ($)"),
    sum("Airport_fee").alias("Airport Fee ($)"),
    sum("total_amount").alias("Total Revenue ($)")
).orderBy(col("Total Revenue ($)").desc())

grouped.show(truncate=False)

spark.stop()
