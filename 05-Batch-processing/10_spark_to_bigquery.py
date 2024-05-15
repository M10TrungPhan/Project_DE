
from pyspark.sql import SparkSession
import argparse

parser = argparse.ArgumentParser()

parser.add_argument('--input_green', required=True)
parser.add_argument('--input_yellow', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

input_green = args.input_green
input_yellow = args.input_yellow
output = args.output

spark = SparkSession.builder \
    .appName('Python script') \
    .getOrCreate()



df_green = spark.read.parquet(input_green)


df_yellow = spark.read.parquet(input_yellow)




df_green = df_green.withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
                   .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')

df_yellow = df_yellow.withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
                   .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')


comon_columns = []
yellow_columns = set(df_yellow.columns)

for col in df_green.columns:
    if col in yellow_columns:
        comon_columns.append(col)


from pyspark.sql import functions as F



df_green_sel = df_green \
                .select(comon_columns) \
                .withColumn('service_type', F.lit('green'))



df_yellow_sel = df_yellow \
                .select(comon_columns) \
                .withColumn('service_type', F.lit('yellow'))


df_trips_data = df_green_sel.unionAll(df_yellow_sel)






df_trips_data.registerTempTable('trips_data')




df_result = spark.sql("""
SELECT
    -- Reveneue grouping 
    PULocationID AS revenue_zone,
    date_trunc("month", "pickup_datetime") AS revenue_month, 
    service_type, 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,

    -- Additional calculations
    AVG(pASsenger_count) AS AVG_monthly_pASsenger_count,
    AVG(trip_distance) AS AVG_monthly_trip_distance

FROM trips_data
GROUP BY 1,2,3
""")




df_result.write.format('bigquerry') \
        .option('table', output) \
        .save()





