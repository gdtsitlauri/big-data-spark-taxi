# Results

Every time below comes from the Spark event logs of one run per version, on the shared Kubernetes cluster
with HDFS storage (Spark History Server). A job has a shuffle-map stage, which reads the data and does the
aggregation or join, and a result stage, which collects or shows the output. The total is the sum of the two.
Machine-readable copy: [`results.csv`](results.csv).

## Q1: average pickup coordinates per hour of the day

Input: the 2015 trips, CSV.

| implementation | shuffle stage (s) | result stage (s) | total (s) |
| --- | ---: | ---: | ---: |
| RDD | 0.335 | 0.347 | 0.682 |
| DataFrame | 89.766 | 0.336 | 90.102 |
| DataFrame with a UDF | 63.731 | 0.368 | 64.099 |

## Q2: per vendor, the longest trip (Haversine distance) and the average duration

Input: the 2015 trips, CSV.

| implementation | shuffle stage (s) | result stage (s) | total (s) |
| --- | ---: | ---: | ---: |
| RDD | 320.317 | 0.750 | 321.127 |
| DataFrame | 70.406 | 0.882 | 71.288 |
| SQL | 113.280 | 0.686 | 113.966 |

The RDD total is the logged one; its two stages add up to 321.067 s.

## Q3: trips per pickup borough, CSV against Parquet

Input for Q3 to Q5: the 2024 trips, as CSV and as Parquet.

| implementation | input | shuffle stage (s) | result stage (s) | total (s) |
| --- | --- | ---: | ---: | ---: |
| DataFrame | CSV | 201.303 | 0.793 | 202.096 |
| DataFrame | Parquet | 10.273 | 0.359 | 10.632 |
| SQL | CSV | 156.786 | 1.632 | 158.418 |
| SQL | Parquet | 11.843 | 0.269 | 12.112 |

Parquet is 19.0 times faster with the DataFrame API and 13.1 times with SQL.

## Q4: night trips (20:00 to 06:00) per vendor, SQL

| input | shuffle stage (s) | result stage (s) | total (s) |
| --- | ---: | ---: | ---: |
| CSV | 193.963 | 0.370 | 194.333 |
| Parquet | 25.373 | 0.658 | 26.031 |

Parquet is 7.5 times faster. The CSV scan used 34 partitions, the Parquet scan 16: predicate pushdown skips
data without night trips, and only the vendor and time columns are read.

## Q5: the 20 busiest pickup-dropoff zone pairs, DataFrame

| input | shuffle stage (s) | result stage (s) | total (s) |
| --- | ---: | ---: | ---: |
| CSV | 190.040 | 1.551 | 191.591 |
| Parquet | 16.784 | 0.852 | 17.636 |

Parquet is 10.9 times faster.

## Q6: revenue per borough with the same 8 cores and 16 GB split differently

Input: the 2024 trips in Parquet and the zone table in CSV, broadcast to the executors.

| executors | shuffle stage (s) | result stage (s) | total (s) |
| --- | ---: | ---: | ---: |
| 2 x (4 cores, 8 GB) | 28.145 | 0.976 | 29.121 |
| 4 x (2 cores, 4 GB) | 20.177 | 1.993 | 22.170 |
| 8 x (1 core, 2 GB) | 15.231 | 0.321 | 15.552 |

Eight small executors are 1.87 times faster than two large ones.

## 1B: join strategy chosen by Catalyst

Input: the 2024 trips and the zone table, both Parquet.

| plan | shuffle stage (s) | result stage (s) | total (s) |
| --- | ---: | ---: | ---: |
| broadcast hash join of the trips with the zone table | 9.391 | 0.317 | 9.708 |

The zone table is about 4.0 MiB in the log ("TorrentBroadcast ... estimated total size 4.0 MiB"), under the
default `spark.sql.autoBroadcastJoinThreshold` of 10 MB, so Catalyst broadcasts it and scans the trip table
once instead of sorting both sides as a sort-merge join would.

## CSV to Parquet conversion (`csv_to_parquet.py`, 2024 trips)

| step | time |
| --- | ---: |
| write to Parquet (200 tasks on 6 executors, 3 to 6.3 s per task) | 210.873 s |
| commit of the Parquet files | 1.886 s |
| validation read (one-partition scan) | 0.610 s |

The 2015 and 2024 trip tables were written in 200 partitions, the zone table in one.
