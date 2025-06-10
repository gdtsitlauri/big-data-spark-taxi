from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Q4_SQL_Parquet").getOrCreate()

df = spark.read.parquet("hdfs://hdfs-namenode:9000/user/gtsitlaouri/data/parquet/yellow_tripdata_2024")

df.createOrReplaceTempView("trips")

result = spark.sql("""
    SELECT 
        VendorID,
        COUNT(*) AS NightTrips
    FROM (
        SELECT 
            VendorID,
            HOUR(tpep_pickup_datetime) AS hour
        FROM trips
    )
    WHERE hour >= 23 OR hour < 7
    GROUP BY VendorID
    ORDER BY VendorID
""")

result.show(truncate=False)
spark.stop()
