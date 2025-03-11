
#start producer
python3 producer.py

#start consumer
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 consumer.py