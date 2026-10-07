# Big Data Analysis of NYC Taxi Trips with Spark

**How much do the choice of Spark API, the file format and the executor layout change the time of the same queries on millions of taxi trips?**

Six queries over the New York City yellow-taxi trip records (TLC, 2015 and 2024), run with Apache Spark
on a Kubernetes cluster with HDFS storage. Each query is written with more than one Spark API (RDD,
DataFrame, Spark SQL) or input format (CSV, Parquet), and the execution times are compared. A seventh
experiment looks at how Spark's Catalyst optimizer chooses a join strategy. A team project for a course
at the University of Thessaly; `report.pdf` (in Greek) has the full analysis.

| query | what it computes | compared |
| --- | --- | --- |
| Q1 | average pickup coordinates per hour of the day | RDD, DataFrame, DataFrame with a UDF |
| Q2 | per vendor, the longest trip (Haversine distance) and related averages | RDD, DataFrame, SQL |
| Q3 | number of trips per pickup borough | DataFrame and SQL, each on CSV and on Parquet |
| Q4 | night trips (20:00 to 06:00) per vendor | SQL on CSV and on Parquet |
| Q5 | the busiest pickup-dropoff zone pairs | DataFrame on CSV and on Parquet |
| Q6 | revenue per borough (fares, tips, tolls, fees) | 2, 4 and 8 executors with the same total resources |
| 1B | join of the trips with the zone table | join strategy chosen by Catalyst |

## Main results

Times are the stage times from the Spark logs, as given in `report.pdf`.

1. **Parquet is 7.5 to 19 times faster than CSV.** Reading the columnar Parquet files instead of CSV cut every
   query that was run both ways:

   | query | CSV | Parquet | faster |
   | --- | ---: | ---: | ---: |
   | Q3, DataFrame | 202.1 s | 10.6 s | 19× |
   | Q3, SQL | 158.4 s | 12.1 s | 13× |
   | Q4, SQL | 194.3 s | 26.0 s | 7.5× |
   | Q5, DataFrame | 191.6 s | 17.6 s | 11× |

2. **The best API depends on the query.** In Q2 the DataFrame version took 71.3 s, SQL 114.0 s and RDD
   321.1 s. In Q3 the DataFrame and SQL versions were close on Parquet (10.6 s and 12.1 s), while on CSV SQL
   was faster (158.4 s against 202.1 s). In Q1 the logged stages of the RDD version took 0.7 s against
   64.1 s (DataFrame with a UDF) and 90.1 s (DataFrame).
3. **More, smaller executors were faster.** With the same total of 8 cores and 16 GB, Q6 took 28.1 s on
   2 executors (4 cores, 8 GB each), 20.2 s on 4 and 15.2 s on 8 executors (1 core, 2 GB each), 1.85 times
   faster.
4. **Catalyst broadcasts the small table.** For the join of the trips with the zone table (about 4 MiB,
   under the 10 MB `spark.sql.autoBroadcastJoinThreshold`), the optimizer chose a broadcast hash join,
   which avoids shuffling the large trip table.

## Limitations (reported as such)

- The report gives one time per version, measured on a shared course cluster, so differences of a few
  seconds should not be read too closely; the effects of the file format and of the executor layout are
  large enough not to depend on them.

## Folder map

```
big-data-spark-taxi/
  csv_to_parquet.py           converts the CSV trip and zone files on HDFS to Parquet (run first)
  queries/
    q1_rdd.py, q1_df.py, q1_df_udf.py
    q2_rdd.py, q2_df.py, q2_sql.py
    q3_df_csv.py, q3_df_parquet.py, q3_sql_csv.py, q3_sql_parquet.py
    q4_sql_csv.py, q4_sql_parquet.py
    q5_df_csv.py, q5_df_parquet.py
    q6_df.py                  Q6; the executor layout is set with spark-submit
    join_strategy.py          1B: the join strategy chosen by Catalyst
  report.pdf                  the full report (in Greek)
```

## Running

Apache Spark 3.5 or later with HDFS (Hadoop 3.3 or later), and the TLC trip records and taxi zone lookup
table on HDFS. The scripts read from `hdfs://hdfs-namenode:9000/user/<user>/...`; change the paths to
yours.

```bash
spark-submit --master k8s://<api-server> --deploy-mode cluster csv_to_parquet.py
spark-submit --master k8s://<api-server> --deploy-mode cluster queries/q3_df_parquet.py

# Q6 with 8 executors of 1 core and 2 GB
spark-submit --master k8s://<api-server> --deploy-mode cluster \
  --conf spark.executor.instances=8 --conf spark.executor.cores=1 --conf spark.executor.memory=2g queries/q6_df.py
```

Data: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

## Authors and license

George David Tsitlauri and Nikiforos Planakis, University of Thessaly. MIT license ([LICENSE](LICENSE)).
