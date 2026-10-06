from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("JoinExplain").getOrCreate()

tripDF = spark.read.parquet("hdfs://hdfs-namenode:9000/user/gtsitlaouri/data/parquet/yellow_tripdata_2024")
zoneDF = spark.read.parquet("hdfs://hdfs-namenode:9000/user/gtsitlaouri/data/parquet/taxi_zone_lookup")

tripDF.createOrReplaceTempView("trips")
zoneDF.createOrReplaceTempView("zones")

query = """
SELECT z1.Borough AS PickupBorough, z2.Borough AS DropoffBorough, *
FROM (
    SELECT * FROM trips LIMIT 50
) t
JOIN zones z1 ON t.PULocationID = z1.LocationID
JOIN zones z2 ON t.DOLocationID = z2.LocationID
"""

df = spark.sql(query)

df.explain(True)  
df.show()
