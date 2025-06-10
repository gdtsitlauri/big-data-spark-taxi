from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Q5_DF_CSV").getOrCreate()

trips = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("hdfs://hdfs-namenode:9000/data/yellow_tripdata_2024.csv")
)

zones = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("hdfs://hdfs-namenode:9000/data/taxi_zone_lookup.csv")
)

pu_zones = zones.withColumnRenamed("LocationID", "PU_LocationID") \
                .withColumnRenamed("Zone", "PickupZone")

do_zones = zones.withColumnRenamed("LocationID", "DO_LocationID") \
                .withColumnRenamed("Zone", "DropoffZone")

joined = trips.join(pu_zones, trips.PULocationID == pu_zones.PU_LocationID) \
              .join(do_zones, trips.DOLocationID == do_zones.DO_LocationID) \
              .filter(col("PU_LocationID") != col("DO_LocationID"))

result = joined.groupBy("PickupZone", "DropoffZone") \
               .count() \
               .withColumnRenamed("count", "TotalTrips") \
               .orderBy(col("TotalTrips").desc())

result.show(20, truncate=False)

spark.stop()
