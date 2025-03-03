from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("HelloWorld") \
    .getOrCreate()

# Create a simple DataFrame with one column and one row
data = [("Hello World")]
columns = ["message"]

df = spark.createDataFrame(data, columns)

# Show the DataFrame
df.show()

# Stop the SparkSession
spark.stop()