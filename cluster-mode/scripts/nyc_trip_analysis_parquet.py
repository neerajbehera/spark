#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
import os
import time

# Set Hadoop user
os.environ["HADOOP_USER_NAME"] = "spark"

# Initialize Spark session with enhanced logging
spark = SparkSession.builder \
    .appName("NYC Taxi Trip Analysis from Parquet") \
    .config("spark.hadoop.fs.hdfs.impl.disable.cache", "true") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

# Start time tracking
start_time = time.time()

# Load the Parquet file with logging
print("\n=== Loading data ===")
print(f"Loading Parquet file from: file:///data/yellow_tripdata_2025-01.parquet")
df = spark.read.parquet("file:///data/yellow_tripdata_2025-01.parquet")
print(f"Initial partition count: {df.rdd.getNumPartitions()}")
print(f"Initial data count: {df.count()}")

# Filter out invalid data with logging
print("\n=== Filtering data ===")
df_filtered = df.filter((col("fare_amount") > 0) & (col("passenger_count") > 0))
print(f"Row count after filtering: {df_filtered.count()}")

# Repartition with detailed logging
print("\n=== Repartitioning data ===")
print("Before repartitioning:")
print(f"Partition count: {df_filtered.rdd.getNumPartitions()}")
print("Sample partition sizes:")
df_filtered.rdd.mapPartitions(lambda x: [sum(1 for _ in x)]).collect()

df_partitioned = df_filtered.repartition(8, col("PULocationID"))
print("\nAfter repartitioning by PULocationID:")
print(f"[PULocationID] Partition count: {df_partitioned.rdd.getNumPartitions()}")
print("[PULocationID] Partition sizes:")
partition_sizes = df_partitioned.rdd.mapPartitions(lambda x: [sum(1 for _ in x)]).collect()
for i, size in enumerate(partition_sizes):
    print(f"[PULocationID]Partition {i}: {size} rows")


partition_counts = df_filtered.rdd.mapPartitionsWithIndex(
    lambda idx, records: [(idx, sum(1 for _ in records))]
).collect()

# Sort and show top 30
sorted_counts = sorted(partition_counts, key=lambda x: x[1], reverse=True)[:30]
print(f"\nTop 30 Partitions by Size: Total {sorted_counts}")
for idx, count in sorted_counts:
    print(f"Partition {idx}: {count:,} records")



# Compute average fare amount with logging
print("\n=== Calculating average fares ===")
avg_fares = df_partitioned.groupBy("PULocationID") \
    .agg(avg("fare_amount").alias("avg_fare"))
print(f"Number of unique PULocationIDs: {avg_fares.count()}")

# Write result with logging
print("\n=== Writing output ===")
output_path = "/data/output_avg_fares_parquet"
print(f"Writing results to: {output_path}")
avg_fares.write.mode("overwrite").parquet(output_path)

# Performance metrics
end_time = time.time()
print("\n=== Job Summary ===")
print(f"Total execution time: {end_time - start_time:.2f} seconds")
print(f"Final partition count: {avg_fares.rdd.getNumPartitions()}")
print("Job completed successfully!")

spark.stop()