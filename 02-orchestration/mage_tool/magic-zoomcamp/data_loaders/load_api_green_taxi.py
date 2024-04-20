import io
import pandas as pd
import requests
from datetime import datetime

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    execution_date = kwargs.get('execution_date')
    format_time = execution_date.strftime("%Y-%m")
    # format_time = '2022-03'
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{format_time}.parquet'
    # print(format_time)
    # ftime

    taxi_dtypes = {
        'VendorID': pd.Int64Dtype(),
        'passenger_count': pd.Int64Dtype(),
        'trip_distance': float,
        'RatecodeID': pd.Int64Dtype(),
        'store_and_fwd_flag': str,
        'PULocationID': pd.Int64Dtype(),
        'DOLocationID': pd.Int64Dtype(),
        'payment_type': pd.Int64Dtype(),
        'fare_amount': float,
        'extra': float,
        'mta_tax': float,
        'tip_amount': float,
        'tolls_amount': float,
        'improvement_surcharge': float,
        'total_amount': float,
        'congestion_surcharge': float 
    }
    parse_dates = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']
    df_result = pd.read_parquet(url)
    df_result = df_result.astype(taxi_dtypes)    
    return df_result


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
