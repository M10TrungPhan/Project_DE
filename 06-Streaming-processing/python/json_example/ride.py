from typing import List, Dict
from decimal import Decimal
from datetime import datetime
from settings import *

class Ride:
    def __init__(self, arr: List[str]):
        self.vendor_id = arr[0]
        self.tpep_pickup_datetime = datetime.strptime(arr[1], "%Y-%m-%d %H:%M:%S"),
        self.tpep_dropoff_datetime = datetime.strptime(arr[2], "%Y-%m-%d %H:%M:%S"),
        self.passenger_count = int(arr[3])
        self.trip_distance = Decimal(arr[4])
        self.rate_code_id = int(arr[5])
        self.store_and_fwd_flag = arr[6]
        self.pu_location_id = int(arr[7])
        self.do_location_id = int(arr[8])
        self.payment_type = arr[9]
        self.fare_amount = Decimal(arr[10])
        self.extra = Decimal(arr[11])
        self.mta_tax = Decimal(arr[12])
        self.tip_amount = Decimal(arr[13])
        self.tolls_amount = Decimal(arr[14])
        self.improvement_surcharge = Decimal(arr[15])
        self.total_amount = Decimal(arr[16])
        self.congestion_surcharge = Decimal(arr[17])

    @classmethod
    def from_dict(cls, d: Dict):
        return cls(arr=[
            d['vendor_id'],
            d['tpep_pickup_datetime'][0],
            d['tpep_dropoff_datetime'][0],
            d['passenger_count'],
            d['trip_distance'],
            d['rate_code_id'],
            d['store_and_fwd_flag'],
            d['pu_location_id'],
            d['do_location_id'],
            d['payment_type'],
            d['fare_amount'],
            d['extra'],
            d['mta_tax'],
            d['tip_amount'],
            d['tolls_amount'],
            d['improvement_surcharge'],
            d['total_amount'],
            d['congestion_surcharge'],
        ]
        )

    def __repr__(self):
        return f'{self.__class__.__name__}: {self.__dict__}'

if __name__ == "__main__":
    import csv 
    import json
    # PIPELINES:
    # READ CSV FILE -> CONVERT TO RIDE OBJECTS -> ENCODE MESSAGE (DUMPS OBJECT) 
    # -> PRODUCE MESSAGE -> CONSUME MESSAGE -> DECODE MESSAGE (LOADS MESSAGE)
    # -> CONVERT MESSAGE TO RIDE OBJECT FOR EASILY PROCESSING
    
    with open(INPUT_DATA_PATH, 'r') as f:
        reader = csv.reader(f)
        header = next(reader)
        # print(header)
        for row in reader:
            # print(row)
            rides = Ride(arr=row)
            
            rides_dict = rides.__dict__
            ride_process_before_produce = json.dumps(rides_dict, default=str)

            ride_process_after_cosumer = json.loads(ride_process_before_produce)
            
            ride_object = Ride.from_dict(ride_process_after_cosumer)
            

            break