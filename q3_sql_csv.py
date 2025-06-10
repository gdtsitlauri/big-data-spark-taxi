from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Q3_SQL_CSV").getOrCreate()

trips = spark.read.csv("hdfs://hdfs-namenode:9000/data/yellow_tripdata_2024.csv", header=True, inferSchema=True)
zones = spark.read.csv("hdfs://hdfs-namenode:9000/data/taxi_zone_lookup.csv", header=True, inferSchema=True)

trips.createOrReplaceTempView("trips")
zones.createOrReplaceTempView("zones")

query = """
SELECT z1.Borough AS Borough, COUNT(*) AS TotalTrips
FROM trips t
JOIN zones z1 ON t.PULocationID = z1.LocationID
JOIN zones z2 ON t.DOLocationID = z2.LocationID
WHERE z1.Borough = z2.Borough
GROUP BY z1.Borough
ORDER BY TotalTrips DESC
"""

result = spark.sql(query)
result.show(truncate=False)
spark.stop()
