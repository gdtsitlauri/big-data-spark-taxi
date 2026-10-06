from pyspark.sql import SparkSession
from pyspark.sql.functions import col, radians, sin, cos, asin, sqrt, expr, unix_timestamp, round, max, struct
from pyspark.sql.types import DoubleType


spark = SparkSession.builder.appName("Q2_SQL").getOrCreate()


csv_path = "hdfs://hdfs-namenode:9000/data/yellow_tripdata_2015.csv"

df = spark.read.option("header", True).option("inferSchema", True).csv(csv_path)

df_filtered = df.filter(
    (col("pickup_latitude").between(40.4, 41.0)) &
    (col("pickup_longitude").between(-74.3, -73.6)) &
    (col("dropoff_latitude").between(40.4, 41.0)) &
    (col("dropoff_longitude").between(-74.3, -73.6))
)

df_rad = df_filtered.withColumn("pickup_lat_rad", radians(col("pickup_latitude"))) \
    .withColumn("pickup_lon_rad", radians(col("pickup_longitude"))) \
    .withColumn("drop_lat_rad", radians(col("dropoff_latitude"))) \
    .withColumn("drop_lon_rad", radians(col("dropoff_longitude")))


df_dist = df_rad.withColumn(
    "haversine_km",
    2 * 6371 * asin(sqrt(
        sin((col("drop_lat_rad") - col("pickup_lat_rad")) / 2) ** 2 +
        cos(col("pickup_lat_rad")) * cos(col("drop_lat_rad")) *
        sin((col("drop_lon_rad") - col("pickup_lon_rad")) / 2) ** 2
    ))
)

df_with_duration = df_dist.withColumn(
    "duration_min",
    (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60
)

df_with_duration.createOrReplaceTempView("trips")

query = """
SELECT
    VendorID,
    ROUND(MAX(haversine_km), 2) AS `Max Haversine Distance (km)`,
    ROUND(first(duration_min, true), 2) AS `Duration (min)`
FROM (
    SELECT *,
           RANK() OVER (PARTITION BY VendorID ORDER BY haversine_km DESC) as rnk
    FROM trips
)
WHERE rnk = 1
GROUP BY VendorID
ORDER BY VendorID
"""

result = spark.sql(query)

result.show(truncate=False)

spark.stop()
