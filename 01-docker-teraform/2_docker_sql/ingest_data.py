#!/usr/bin/env python
# coding: utf-8
import pandas as pd
import argparse
from sqlalchemy import create_engine
from time import time
import os

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url


    parquet_name = 'output.parquet'
    csv_name = 'output.csv'

    # os.system(f"curl {url} -o {parquet_name}")
    os.system(f"wget {url} -O {parquet_name}")

    df = pd.read_parquet(parquet_name,
                     engine='pyarrow')

    df.to_csv(csv_name,
            header=True, index=False)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    print(pd.io.sql.get_schema(df, name='yellow_taxi_data'))

    

    df_iter = pd.read_csv(csv_name,iterator=True, chunksize=100000)
    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)


    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name=table_name, con=engine, if_exists='append')

    df = next(df_iter)
    while True:
        t_statrt = time()

        df = next(df_iter)

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.to_sql(name=table_name, con=engine, if_exists='append')

        print(f'Insert another chunk .., took {(time()- t_statrt)} second'  )

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest data CSV to Postgres')
    parser.add_argument('--user', help = 'Username for Postgres')
    parser.add_argument('--password', help = 'Password for Postgres')
    parser.add_argument('--host', help = 'Hostname for Postgres')
    parser.add_argument('--port', help = 'Port for Postgres')
    parser.add_argument('--db', help = 'Database for Postgres')
    parser.add_argument('--table_name', help = 'Table name for database')
    parser.add_argument('--url', help = 'url of the csv file')
    args = parser.parse_args()

    main(args)