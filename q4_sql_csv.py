from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Q4_SQL_CSV").getOrCreate()

df = spark.read.option("header", "true").csv("hdfs://hdfs-namenode:9000/data/yellow_tripdata_2024.csv")

df.createOrReplaceTempView("trips")

result = spark.sql("""
    SELECT 
        VendorID,
        COUNT(*) AS NightTrips
    FROM (
        SELECT 
            VendorID,
            HOUR(TO_TIMESTAMP(tpep_pickup_datetime)) AS hour
        FROM trips
    )
    WHERE hour >= 23 OR hour < 7
    GROUP BY VendorID
    ORDER BY VendorID
""")

result.show(truncate=False)
spark.stop()
