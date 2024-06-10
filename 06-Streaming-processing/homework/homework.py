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

topic_name = 'green-trips-3'
# topic_name = 'green-trips'
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

# query = green_stream.writeStream.foreachBatch(peek).start()
from pyspark.sql import types

schema = types.StructType() \
    .add("lpep_pickup_datetime", types.StringType()) \
    .add("lpep_dropoff_datetime", types.StringType()) \
    .add("PULocationID", types.IntegerType()) \
    .add("DOLocationID", types.IntegerType()) \
    .add("passenger_count", types.DoubleType()) \
    .add("trip_distance", types.DoubleType()) \
    .add("tip_amount", types.DoubleType())

from pyspark.sql import functions as F



green_stream = green_stream \
            .select(F.from_json(F.col('value').cast('STRING'), schema=schema).alias('data')) \
            .select("data.*") \
            
popular_destinations = green_stream \
                    .withColumn('timestamp', F.current_timestamp()) \
                    .groupby('DOLocationID') \
                    .count() \
                    .orderBy('count', ascending=False)

write_query = popular_destinations.writeStream \
        .outputMode('complete') \
        .trigger(processingTime='10 seconds') \
        .option('truncate', False) \
        .format('console') \
        .start()

write_query.awaitTermination()

import time
time.sleep(60)