from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session with Kafka package
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

# Define Kafka source
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test-topic") \
    .option("startingOffsets", "earliest") \
    .load()

# Convert Kafka value (bytes) to string
kafka_df = kafka_df.selectExpr("CAST(value AS STRING)")

# Process the data (example: filter messages containing "Message")
processed_df = kafka_df.filter(col("value").contains("Message"))

# Write the output to the console
query = processed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Wait for the stream to finish
query.awaitTermination()