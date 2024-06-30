import pandas as pd
from sqlalchemy import create_engine
import os 
from time import time

def ingest_data(user, password, host, port, db, table_name, parquet_file):
    csv_name = parquet_file.split("/")[-1].replace('_raw.parquet','_raw.csv')
    # csv_file = parquet_file.replace('_raw.parquet','_raw.csv')
    table_name ="yellow_taxi_data"
    print(user, password, host, port, db, table_name, parquet_file)
    
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    engine.connect()
    df = pd.read_parquet(parquet_file,
                        engine='pyarrow')
    print(len(df))
    df.to_csv(csv_name,
                header=True, index=False)
    
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)
    # print(df.columns)
    df.tpep_pickup_datetime  = pd.to_datetime(df.tpep_pickup_datetime )
    df.tpep_pickup_datetime  = pd.to_datetime(df.tpep_pickup_datetime )
    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')
    # df.to_sql(name=table_name, con=engine, if_exists='replace')
    count = 0
    while count < 3:
        count +=1
        t_statrt = time()

        df = next(df_iter)

        # df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        # df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.tpep_pickup_datetime  = pd.to_datetime(df.tpep_pickup_datetime )
        df.tpep_pickup_datetime  = pd.to_datetime(df.tpep_pickup_datetime )

        df.to_sql(name=table_name, con=engine, if_exists='append')

        print(f'Insert another chunk .., took {(time()- t_statrt)} second'  )