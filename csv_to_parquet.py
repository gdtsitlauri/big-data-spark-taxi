from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, IntegerType, DoubleType,
    StringType, TimestampType
)

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Convert CSVs to Parquet") \
        .getOrCreate()

    # 1) 2015 dataset
    schema_2015 = StructType([
        StructField("VendorID", IntegerType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("pickup_longitude", DoubleType(), True),
        StructField("pickup_latitude", DoubleType(), True),
        StructField("RateCodeID", IntegerType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("dropoff_longitude", DoubleType(), True),
        StructField("dropoff_latitude", DoubleType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True)
    ])

    df2015 = spark.read.csv(
        "hdfs://hdfs-namenode:9000/data/yellow_tripdata_2015.csv",
        header=True,
        schema=schema_2015
    )
    df2015.repartition(200) \
        .write \
        .mode("overwrite") \
        .parquet("hdfs://hdfs-namenode:9000/user/gtsitlaouri/data/parquet/yellow_tripdata_2015")

    # 2) 2024 dataset
    schema_2024 = StructType([
        StructField("VendorID", IntegerType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("RatecodeID", IntegerType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("PULocationID", IntegerType(), True),
        StructField("DOLocationID", IntegerType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
        StructField("Airport_fee", DoubleType(), True),
        # Ίσως υπάρχει και cbd_congestion_fee
        StructField("cbd_congestion_fee", DoubleType(), True),
    ])

    df2024 = spark.read.csv(
        "hdfs://hdfs-namenode:9000/data/yellow_tripdata_2024.csv",
        header=True,
        schema=schema_2024
    )
    df2024.repartition(200) \
        .write \
        .mode("overwrite") \
        .parquet("hdfs://hdfs-namenode:9000/user/gtsitlaouri/data/parquet/yellow_tripdata_2024")

    # 3) taxi_zone_lookup
    schema_zones = StructType([
        StructField("LocationID", IntegerType(), True),
        StructField("Borough", StringType(), True),
        StructField("Zone", StringType(), True),
        StructField("service_zone", StringType(), True),
    ])

    dfzones = spark.read.csv(
        "hdfs://hdfs-namenode:9000/data/taxi_zone_lookup.csv",
        header=True,
        schema=schema_zones
    )
    dfzones.repartition(1) \
        .write \
        .mode("overwrite") \
        .parquet("hdfs://hdfs-namenode:9000/user/gtsitlaouri/data/parquet/taxi_zone_lookup")

    spark.stop()