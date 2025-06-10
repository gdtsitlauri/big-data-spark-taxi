from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Q3_DF_Parquet").getOrCreate()

trips = spark.read.parquet("hdfs://hdfs-namenode:9000/user/gtsitlaouri/data/parquet/yellow_tripdata_2024")
zones = spark.read.parquet("hdfs://hdfs-namenode:9000/user/gtsitlaouri/data/parquet/taxi_zone_lookup")

joined = trips.join(zones.withColumnRenamed("LocationID", "PULocationID").withColumnRenamed("Borough", "PUBorough"), on="PULocationID") \
              .join(zones.withColumnRenamed("LocationID", "DOLocationID").withColumnRenamed("Borough", "DOBorough"), on="DOLocationID")

result = joined.filter("PUBorough = DOBorough") \
               .groupBy("PUBorough") \
               .count() \
               .withColumnRenamed("PUBorough", "Borough") \
               .withColumnRenamed("count", "TotalTrips") \
               .orderBy("TotalTrips", ascending=False)

result.show(truncate=False)
spark.stop()
