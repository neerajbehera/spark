from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
import os

# Initialize Spark with improved config
spark = SparkSession.builder \
    .appName("NYC Trip Output Analysis") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

try:
    # Read the processed output
    df = spark.read.parquet("file:///data/output_avg_fares_parquet")
    
    # Create temporary view for SQL queries
    df.createOrReplaceTempView("avg_fares")
    
    print("\n===== ANALYSIS REPORT =====\n")
    
    # 1. Basic stats
    print("=== Summary Statistics ===")
    spark.sql("""
      SELECT 
        COUNT(*) as total_locations,
        ROUND(MIN(avg_fare), 2) as min_fare,
        ROUND(MAX(avg_fare), 2) as max_fare,
        ROUND(AVG(avg_fare), 2) as avg_fare,
        ROUND(STDDEV(avg_fare), 2) as std_dev
      FROM avg_fares
    """).show(truncate=False)
    
    # 2. Top/Bottom 10 locations
    print("\n=== Top 10 Highest Average Fares ===")
    spark.sql("""
      SELECT PULocationID, ROUND(avg_fare, 2) as avg_fare
      FROM avg_fares 
      ORDER BY avg_fare DESC
      LIMIT 10
    """).show(truncate=False)
    
    print("\n=== Top 10 Lowest Average Fares ===")
    spark.sql("""
      SELECT PULocationID, ROUND(avg_fare, 2) as avg_fare
      FROM avg_fares 
      WHERE avg_fare > 0  # Exclude possible outliers
      ORDER BY avg_fare ASC
      LIMIT 10
    """).show(truncate=False)
    
    # 3. Save analysis results
    print("\nSaving analysis results...")
    spark.sql("""
      SELECT PULocationID, ROUND(avg_fare, 2) as avg_fare
      FROM avg_fares
      ORDER BY avg_fare DESC
    """).write.mode("overwrite").csv("file:///data/analysis_results")
    
    print("\n===== ANALYSIS COMPLETED =====\n")
    
finally:
    spark.stop()