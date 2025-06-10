from pyspark import SparkContext
from datetime import datetime
from math import radians, cos, sin, asin, sqrt

def haversine(lat1, lon1, lat2, lon2):
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371  
    return c * r

def is_valid_nyc_coords(lat, lon):
    return 40.4 <= lat <= 41.0 and -74.3 <= lon <= -73.6

def parse_line(line):
    try:
        parts = line.split(",")

        if parts[0] == "VendorID":
            return None

        vendor = parts[0]
        pickup_time = datetime.strptime(parts[1], "%Y-%m-%d %H:%M:%S")
        dropoff_time = datetime.strptime(parts[2], "%Y-%m-%d %H:%M:%S")

        pickup_lon = float(parts[5])
        pickup_lat = float(parts[6])
        dropoff_lon = float(parts[9])
        dropoff_lat = float(parts[10])

        if not (is_valid_nyc_coords(pickup_lat, pickup_lon) and is_valid_nyc_coords(dropoff_lat, dropoff_lon)):
            return None

        distance = haversine(pickup_lat, pickup_lon, dropoff_lat, dropoff_lon)
        duration = (dropoff_time - pickup_time).total_seconds() / 60  

        return (vendor, (distance, duration))
    except:
        return None

if __name__ == "__main__":
    sc = SparkContext(appName="Q2_RDD")

    input_path = "hdfs://hdfs-namenode:9000/data/yellow_tripdata_2015.csv"
    raw_rdd = sc.textFile(input_path)
    header = raw_rdd.first()
    data_rdd = raw_rdd.filter(lambda line: line != header)

    parsed = data_rdd.map(parse_line).filter(lambda x: x is not None)

    max_per_vendor = parsed.reduceByKey(lambda a, b: a if a[0] > b[0] else b)

    results = max_per_vendor.collect()

    print("VendorID\tMax Haversine Distance (km)\tDuration (min)")
    for vendor, (dist, dur) in sorted(results):
        print(f"{vendor}\t\t{dist:.2f}\t\t\t\t{dur:.1f}")

    sc.stop()
