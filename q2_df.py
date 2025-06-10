from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, max as spark_max, struct
from pyspark.sql.types import FloatType, DoubleType
from math import radians, cos, sin, asin, sqrt

def haversine(lat1, lon1, lat2, lon2):
    """
    Υπολογισμός Haversine απόστασης σε km.
    """
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * asin(sqrt(a))
    r = 6371  
    return c * r

# UDF
haversine_udf = udf(haversine, DoubleType())

def is_valid_coord(lat, lon):
    return 40.4 <= lat <= 41.0 and -74.3 <= lon <= -73.6

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Q2_DF").getOrCreate()

    df = spark.read.option("header", True).csv("hdfs://hdfs-namenode:9000/data/yellow_tripdata_2015.csv")

    df = df.select(
        col("VendorID").cast("int"),
        col("tpep_pickup_datetime").cast("timestamp"),
        col("tpep_dropoff_datetime").cast("timestamp"),
        col("pickup_latitude").cast("float"),
        col("pickup_longitude").cast("float"),
        col("dropoff_latitude").cast("float"),
        col("dropoff_longitude").cast("float")
    )

    df = df.filter(
        (col("pickup_latitude").between(40.4, 41.0)) &
        (col("pickup_longitude").between(-74.3, -73.6)) &
        (col("dropoff_latitude").between(40.4, 41.0)) &
        (col("dropoff_longitude").between(-74.3, -73.6)) &
        (col("VendorID").isNotNull()) &
        (col("tpep_pickup_datetime").isNotNull()) &
        (col("tpep_dropoff_datetime").isNotNull())
    )

    df = df.withColumn("distance_km", haversine_udf(
        col("pickup_latitude"), col("pickup_longitude"),
        col("dropoff_latitude"), col("dropoff_longitude")
    ))

    df = df.withColumn("duration_min",
        (col("tpep_dropoff_datetime").cast("long") - col("tpep_pickup_datetime").cast("long")) / 60.0
    )

    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number

    windowSpec = Window.partitionBy("VendorID").orderBy(col("distance_km").desc())

    df_max = df.withColumn("row_num", row_number().over(windowSpec)) \
               .filter(col("row_num") == 1) \
               .select("VendorID", "distance_km", "duration_min")

    df_max = df_max.orderBy("VendorID")
    df_max = df_max.withColumnRenamed("distance_km", "Max Haversine Distance (km)") \
                   .withColumnRenamed("duration_min", "Duration (min)")

    df_max.show()

    spark.stop()
