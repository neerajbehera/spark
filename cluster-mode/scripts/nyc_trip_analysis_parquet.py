from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

import os


os.environ["HADOOP_USER_NAME"] = "spark"

# Initialize Spark session
spark = SparkSession.builder \
    .appName("NYC Taxi Trip Analysis from Parquet") \
    .config("spark.hadoop.fs.hdfs.impl.disable.cache", "true") \
    .getOrCreate()

# Load the Parquet file
df = spark.read.parquet("file:///data/yellow_tripdata_2025-01.parquet")


# Filter out invalid data
df_filtered = df.filter((col("fare_amount") > 0) & (col("passenger_count") > 0))

# Repartition based on pickup location to optimize parallelism
df_partitioned = df_filtered.repartition(8, col("PULocationID"))

# Compute average fare amount by pickup location
avg_fares = df_partitioned.groupBy("PULocationID") \
    .agg(avg("fare_amount").alias("avg_fare"))

# Write result to disk
avg_fares.write.mode("overwrite").parquet("/data/output_avg_fares_parquet")

spark.stop()
