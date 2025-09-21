
Spark & Hadoop Taxi Trips Analysis
---

## 📑 Contents

1. Introduction
2. Requirements & Environment
3. File Structure
4. Execution Instructions
   * CSV → Parquet Conversion
   * Query Q1: RDD / DataFrame
   * Query Q2: RDD / DataFrame
   * Query Q3: DataFrame & SQL (CSV vs Parquet)
   * Query Q4: SQL (CSV vs Parquet)
   * Query Q5: DataFrame (CSV vs Parquet)
   * Query Q6: DataFrame + Scaling
   * Part 1B: Optimizer Join Study
5. Observations & Performance Comparison

---

## Introduction

Analysis of real NYC TLC taxi trips data using **Apache Spark** on **HDFS**.  
Queries Q1–Q6 are implemented with RDD, DataFrame, SQL APIs and CSV vs Parquet formats, as well as a study of the join strategy of the Catalyst optimizer.

---

## Requirements & Environment

* Apache Hadoop ≥ 3.3
* Apache Spark ≥ 3.5
* Python 3.8+
* Kubernetes cluster & HDFS access
* Spark submit from Docker image `apache/spark`
* Settings in `spark-defaults.conf` (namespace, serviceAccount, etc.)

---

## File Structure

```
.
├── csv_to_parquet.py            # Converts all CSV files to Parquet
├── 1b.py                        # Part 1B: explain – join optimizer study
│
├── q1_rdd.py                    # Q1 – RDD API
├── q1_df.py                     # Q1 – DataFrame API (no UDF)
├── q1_df_udf.py                 # Q1 – DataFrame API (with UDF)
│
├── q2_rdd.py                    # Q2 – RDD API
├── q2_df.py                     # Q2 – DataFrame API
├── q2_sql.py                    # Q2 – SparkSQL API
│
├── q3_df_csv.py                 # Q3 – DataFrame API (CSV)
├── q3_df_parquet.py             # Q3 – DataFrame API (Parquet)
├── q3_sql_csv.py                # Q3 – SQL API (CSV)
└── q3_sql_parquet.py            # Q3 – SQL API (Parquet)

├── q4_sql_csv.py                # Q4 – SQL API (CSV)
├── q4_sql_parquet.py            # Q4 – SQL API (Parquet)
│
├── q5_df_csv.py                 # Q5 – DataFrame API (CSV)
└── q5_df_parquet.py             # Q5 – DataFrame API (Parquet)

└── q6_df.py                     # Q6 – DataFrame API + scaling tests
│
├── report.pdf
```

---

## Execution Instructions

Replace `<username>` in the HDFS path with your own username.

### CSV → Parquet Conversion

```bash
spark-submit \
  --master k8s://… \
  --deploy-mode cluster \
  hdfs://hdfs-namenode:9000/user/<username>/csv_to_parquet.py
```

---

### Query Q1

1. **RDD API**

   ```bash
   spark-submit … q1_rdd.py
   ```
2. **DataFrame API (no UDF)**

   ```bash
   spark-submit … q1_df.py
   ```
3. **DataFrame API (with UDF)**

   ```bash
   spark-submit … q1_df_udf.py
   ```

---

### Query Q2

* **RDD**:

  ```bash
  spark-submit … q2_rdd.py
  ```
* **DataFrame**:

  ```bash
  spark-submit … q2_df.py
  ```
* **SQL**:

  ```bash
  spark-submit … q2_sql.py
  ```

---

### Query Q3

* **DataFrame, CSV**:

  ```bash
  spark-submit … q3_df_csv.py
  ```
* **DataFrame, Parquet**:

  ```bash
  spark-submit … q3_df_parquet.py
  ```
* **SQL, CSV**:

  ```bash
  spark-submit … q3_sql_csv.py
  ```
* **SQL, Parquet**:

  ```bash
  spark-submit … q3_sql_parquet.py
  ```

---

### Query Q4

* **SQL, CSV**:

  ```bash
  spark-submit … q4_sql_csv.py
  ```
* **SQL, Parquet**:

  ```bash
  spark-submit … q4_sql_parquet.py
  ```

---

### Query Q5

* **DataFrame, CSV**:

  ```bash
  spark-submit … q5_df_csv.py
  ```
* **DataFrame, Parquet**:

  ```bash
  spark-submit … q5_df_parquet.py
  ```

---

### Query Q6

```bash
# Horizontal & vertical scaling experiments:
#   • 2 executors × 4 cores / 8 GB
#   • 4 executors × 2 cores / 4 GB
#   • 8 executors × 1 core  / 2 GB
spark-submit \
  --conf spark.executor.instances=<n> \
  --conf spark.executor.cores=<c> \
  --conf spark.executor.memory=<m> \
  … q6_df.py
```

---

### Part 1B: Optimizer Join Study

```bash
spark-submit … 1b.py
```

In the script, `spark.sql("EXPLAIN …")` is called for 50 records and then time and join type (Broadcast vs Shuffle) are measured.

---

## License & References

* Data: NYC TLC Trip Record Data
* Apache Spark & Hadoop documentation
* Parquet format: [https://parquet.apache.org/](https://parquet.apache.org/)
