import pyspark 
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
pyspark_version = pyspark.__version__
kafka_jar_package = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark_version}"
# SparkSession.setLevel('INFO')
spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("Green Trips Consumer") \
    .config('spark.jars.packages', kafka_jar_package) \
    .config('spark.sql.streaming.statefulOperator.checkCorrectness.enabled', False) \
    .getOrCreate()

topic_name = 'green-trips-3'
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



extract_sample = green_stream \
            .select(F.from_json(F.col('value').cast('STRING'), schema=schema).alias('data')) \
            .select("data.*") \
            
# query_1 = extract_sample \
#                     .withColumn('timestamp', F.current_timestamp()) \
#                     .groupby('DOLocationID') \
#                     .count() \
#                     .orderBy('count', ascending=False)

# query_1.writeStream \
#         .outputMode('complete') \
#         .trigger(processingTime='30 seconds') \
#         .option('truncate', False) \
#         .format('console') \
#         .start().awaitTermination()

# query_2 = extract_sample.writeStream \
#                     .outputMode('append') \
#                     .trigger(processingTime='10 seconds') \
#                     .option('truncate', False) \
#                     .format('console') \
#                     .start().awaitTermination()


# extract_sample_2 = green_stream \
#             .select(F.from_json(F.col('value').cast('STRING'), schema=schema).alias('data'), 'timestamp') \
#             .select("data.PULocationID","data.DOLocationID","data.trip_distance", 'timestamp')

# query_3 = extract_sample_2 \
#             .groupBy(window('timestamp', '1 minutes'), 'PULocationID') \
#             .count().sort(desc("window"), desc("count"))

# query_3.writeStream.outputMode('complete').format('console').start(truncate=False).awaitTermination()

lookup_static_df = spark.read.options(header=True).csv('../../05-Batch-processing/taxi_zone_lookup.csv')

extract_sample_3 = green_stream \
            .select(F.from_json(F.col('value').cast('STRING'), schema=schema).alias('data'), 'timestamp') \
            .select("data.PULocationID","data.DOLocationID","data.trip_distance", F.current_timestamp().alias('timestamp'))

# query_4 = extract_sample_3 \
#             .groupBy(window('timestamp', '5 minutes'), 'DOLocationID') \
#             .count().sort(desc("window"), desc("count"))

# query_4.writeStream.outputMode('complete').format('console').start(truncate=False).awaitTermination()


query_5 = extract_sample_3.alias('t1') \
                .join(lookup_static_df.alias('t2'), col('t1.DOLocationID') == col('t2.LocationID')) \
                .select('DOLocationID','Zone', 'timestamp') \
                .groupBy(window('timestamp', '5 minutes').alias('timestamp'),  'Zone') \
                .count() \
                .sort(desc("timestamp"), desc("count")).limit(1)
query_5.writeStream.outputMode('complete').format('console').start(truncate=False).awaitTermination()

# import time
# time.sleep(60)