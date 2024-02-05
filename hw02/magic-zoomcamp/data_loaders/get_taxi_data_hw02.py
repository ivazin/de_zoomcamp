import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):

    taxi_dtypes = {
                    'VendorID': pd.Int64Dtype(),
                    'passenger_count': pd.Int64Dtype(),
                    'trip_distance': float,
                    'RatecodeID':pd.Int64Dtype(),
                    'store_and_fwd_flag':str,
                    'PULocationID':pd.Int64Dtype(),
                    'DOLocationID':pd.Int64Dtype(),
                    'payment_type': pd.Int64Dtype(),
                    'fare_amount': float,
                    'extra':float,
                    'mta_tax':float,
                    'tip_amount':float,
                    'tolls_amount':float,
                    'improvement_surcharge':float,
                    'total_amount':float,
                    'congestion_surcharge':float
                }

    # parse_dates = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']
    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

    input_list = ['2020-10', '2020-11', '2020-12']

    df = pd.DataFrame()

    for item in input_list:
        # url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_{item}.csv.gz'
        url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_{item}.csv.gz'
        print("Parsing:", url)
        df_addon = pd.read_csv(
            url, sep=',', compression='gzip', dtype=taxi_dtypes, parse_dates=parse_dates
        )
        df = pd.concat([df, df_addon], ignore_index=True)
        print("Shape:", *df.shape)

    print(df['VendorID'].value_counts())
    return df


@test
def test_output(output, *args) -> None:
    assert output is not None, 'The output is undefined'