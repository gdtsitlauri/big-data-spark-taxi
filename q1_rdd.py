from pyspark import SparkContext
from datetime import datetime

def parse_line(line):
    parts = line.split(",")
    try:
        if parts[0] == "VendorID":
            return None
        pickup_datetime = parts[1]
        lat = float(parts[5])
        lon = float(parts[6])
        if lat == 0.0 or lon == 0.0:
            return None

        hour = datetime.strptime(pickup_datetime, "%Y-%m-%d %H:%M:%S").hour
        return (hour, (lat, lon))
    except:
        return None

if __name__ == "__main__":
    sc = SparkContext(appName="Q1_RDD")

    input_path = "hdfs://hdfs-namenode:9000/data/yellow_tripdata_2015.csv"
    raw = sc.textFile(input_path)
    header = raw.first()
    data = raw.filter(lambda line: line != header)

    parsed = data.map(parse_line).filter(lambda x: x is not None)

    sum_count = parsed.mapValues(lambda coords: (coords[0], coords[1], 1)) \
                      .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2]))

    avg_per_hour = sum_count.mapValues(lambda v: (v[0] / v[2], v[1] / v[2]))

    result = avg_per_hour.sortByKey().collect()

    print("HourOfDay\tLatitude\tLongitude")
    for hour, (avg_lat, avg_lon) in result:
        print(f"{hour:02d}\t\t{avg_lat:.6f}\t{avg_lon:.6f}")

    sc.stop()
