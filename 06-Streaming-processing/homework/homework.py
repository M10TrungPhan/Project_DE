import pyspark 
from pyspark.sql import SparkSession

pyspark_version = pyspark.__version__
kafka_jar_package = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark_version}"
# SparkSession.setLevel('INFO')
spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("Green Trips Consumer") \
    .config('spark.jars.packages', kafka_jar_package) \
    .getOrCreate()

topic_name = 'test-topic'
topic_name = 'green-trips'
server = 'redpanda-1:9092'
server = 'localhost:9092'
green_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", server) \
    .option("subscribe", topic_name) \
    .option('startingOffsets', 'earliest') \
    .load()

green_stream.printSchema()
def  peek(mini_batch, batch_id):
    first_row = mini_batch.take(1)

    if first_row:
        print(first_row[0])

# query = green_stream.writeStream.foreachBatch(peek)
# query.start()
# query.awaitTermination()

query = green_stream.writeStream.foreachBatch(peek).start()
import time
time.sleep(60)