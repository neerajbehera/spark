from pyspark.sql import SparkSession



spark = SparkSession.builder.appName("SampleView").getOrCreate()

data = [("neeraj",39), ("james", 40), ("john", 30)]

columns = ["name", "age"]

df = spark.createDataFrame(data, columns)

df.createOrReplaceTempView("people")

result = spark.sql("select * from people where age >30")
result.show()
