from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Q3_SQL_Parquet").getOrCreate()

trips = spark.read.parquet("hdfs://hdfs-namenode:9000/user/gtsitlaouri/data/parquet/yellow_tripdata_2024")
zones = spark.read.parquet("hdfs://hdfs-namenode:9000/user/gtsitlaouri/data/parquet/taxi_zone_lookup")

trips.createOrReplaceTempView("trips")
zones.createOrReplaceTempView("zones")

result = spark.sql("""
  SELECT z1.Borough AS Borough, COUNT(*) AS TotalTrips
  FROM trips t
  JOIN zones z1 ON t.PULocationID = z1.LocationID
  JOIN zones z2 ON t.DOLocationID = z2.LocationID
  WHERE z1.Borough = z2.Borough
  GROUP BY z1.Borough
  ORDER BY TotalTrips DESC
""")

result.show(truncate=False)
spark.stop()
