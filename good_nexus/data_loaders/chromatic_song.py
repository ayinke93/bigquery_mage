import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs):
    urls = ['https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-01.parquet', 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-02.parquet', 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-03.parquet', 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-04.parquet', 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-05.parquet', 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-06.parquet' , 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-07.parquet', 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-08.parquet', 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-09.parquet' , 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-10.parquet' , 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-11.parquet', 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-12.parquet']

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

    parse_date = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

    dataframes = []

    for url in urls:
        df = pd.read_parquet(url )

        dataframes.append(df)

    data = pd.concat(dataframes , ignore_index = True)

    print(data.dtypes)





